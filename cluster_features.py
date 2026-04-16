import pandas as pd
import numpy as np
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.cluster import KMeans
from sklearn.metrics import silhouette_score
import matplotlib.pyplot as plt
from pathlib import Path
import textwrap
import cga_utils
import math
from typing import Iterable, List, Optional, Tuple
import hdbscan
from sklearn.feature_extraction import DictVectorizer

def err_bucket(rel_err):
    if rel_err < 1e-6: 
        return 0
    elif rel_err < 0.01:
        return 1
    elif rel_err < 0.1:
        return 2
    elif rel_err < 1:
        return 3
    else:
        return 4

def value_bucket(ans, pred):
    if ans == pred:
        return 0
    if ans == -1*pred:
        return 1
    if math.isnan(pred):
        return 2
    else: 
        return 3

def prepare_matrix(errors, features):
    EPS = 1e-9
    
    errors["rel_err"] = errors.apply(lambda row: (row["pred"] - row["answer"]) / max(row["answer"], EPS), axis=1  )
    errors["abs_err"] = errors.apply(lambda row: row["pred"] - row["answer"], axis=1  )
    errors["magnitude_bucket"] = errors.apply(lambda row: int(math.floor(math.log10(abs(row["answer"])+EPS))) if row["answer"] != 0 else -1, axis=1  )
    errors["rel_error_bucket"] = errors["rel_err"].apply(err_bucket)
    errors["ratio"] = errors.apply(lambda row: (row["pred"] / (row["answer"]+EPS)) if row["answer"] != 0 else np.inf,  axis=1 )
    errors["x100_flag"] = errors["ratio"].apply(lambda ratio: int(0.95 < ratio/100 < 1.05 or 0.95 < ratio*100 < 1.05))
    errors["has_error_text"] = errors["error_text"].apply(lambda txt: txt != None and txt != '')
    errors["ratio_is_inf"] = np.isinf(errors["ratio"]).astype(int)
    errors["ratio"] = np.where(np.isinf(errors["ratio"]), np.nan, errors["ratio"])
    
    errors["scale_mismatch"]= errors["scale"] != errors["pred_scale"]
    errors["value_mismatch"]= errors["answer"] != errors["pred"]
    errors["value_nan"]= errors["pred"].isna()
    errors["value_match"]= errors.apply(lambda row: value_bucket(row["answer"], row["pred"]) ,  axis=1 )

    # 2) Erősen ferde oszlopok log1p-vel és/vagy winsorize/clip
    for col in ["abs_err", "rel_err", "ratio"]:
        if col in errors:
            # negatív is lehet -> signed log1p
            errors[col] = np.sign(errors[col]) * np.log1p(np.abs(errors[col]))

    # 3) NaN-ek kezelése (pl. median impute)
    num_cols = ["rel_err","abs_err","ratio"]
    for col in num_cols:
        if col in errors:
            med = errors[col].median()
            errors[col] = errors[col].fillna(med)

    # 4) Skálázás a folytonosokra (RobustScaler a kilógók ellen)
    from sklearn.preprocessing import RobustScaler
    cont = errors[num_cols].values
    scaler = RobustScaler().fit(cont)
    errors[num_cols] = scaler.transform(cont)

    errors["scale"] = errors["scale"].fillna("")
    errors["pred_scale"] = errors["pred_scale"].fillna("")
    errors["code_calc_pattern"]= errors["code_calc_pattern"].fillna("")

    #vects = errors.drop(["ts", "qid", "pred_scale", "question", "derivation", "calc_pattern", "pred", "answer", "scale", "value_match", "error_text",	"value_list", "code",	"selected_values","needed_values","exact_match",	"error_code"], axis=1)
    #vects = errors[["calc_pattern", "error_code",  "pred_ast", "selection_success", "sign_error", "is_parenth_in_table", "has_code_abs",
    # "scale", "pred_scale",  "rel_error_bucket", "magnitude_bucket", "x100_flag",  "has_error_text"]]
    #vects = errors[["calc_pattern", "code_calc_pattern", "scale", "pred_scale"]]
    #vects = errors[["calc_pattern", "code_calc_pattern", "scale", "pred_scale","x100_flag", "sign_error", 'error_code']]
    vects = errors[features]
    X_dict = vects.to_dict(orient="records")
    dv = DictVectorizer(sparse=False)
    X_flags = dv.fit_transform(X_dict)   
    return X_flags

def cluster_hdbscan(X: np.ndarray, min_cluster_size: int = 8, min_samples: int = 2) -> Tuple[np.ndarray, Optional[float]]:
    if hdbscan is None:
        print("[warn] hdbscan not installed; falling back to agglomerative.")
        return cluster_agglomerative(X, min_cluster_size)
    clusterer = hdbscan.HDBSCAN(min_cluster_size=min_cluster_size, min_samples=min_samples, metric="manhattan")
    labels = clusterer.fit_predict(X)
    print('pers: ', clusterer.cluster_persistence_)
    return labels, clusterer.cluster_persistence_, clusterer.probabilities_

from typing import Iterable, List, Optional, Tuple

def try_hdbscan(X, mcs_list=(2,3,4, 5,6, 10,15), ms_list=(1,2,3, 4, 5)):    
    from collections import defaultdict
    out = []
    for mcs in mcs_list:
        for ms in ms_list:
            cl = hdbscan.HDBSCAN(min_cluster_size=mcs, min_samples=ms, metric="manhattan")
            labels = cl.fit_predict(X)
            n_clusters = len(set(labels) - {-1})
            noise_ratio = (labels == -1).mean()
            out.append((mcs, ms, n_clusters, noise_ratio))        
    return pd.DataFrame(out, columns=["min_cluster_size", "min_samples", "n_clusters", "noise_ratio"]).sort_values(by="noise_ratio")

import numpy as np
import pandas as pd
from sklearn.metrics import pairwise_distances

def cluster_compactness_metrics(X, labels, probabilities=None, metric="euclidean", sample_cap=5000):
    """
    X: np.ndarray alakú feature mátrix (n_samples, n_features)
    labels: HDBSCAN clusterer.labels_
    probabilities: HDBSCAN clusterer.probabilities_ (opcionális, súlyozáshoz)
    metric: távolságmetrika a pairwise_distances-hez
    sample_cap: ha egy klaszter nagyon nagy, ennyire limitáljuk a páronkénti O(n^2) számítást (véletlen mintavétel)
    """
    X = np.asarray(X)
    labels = np.asarray(labels)
    if probabilities is None:
        probabilities = np.ones_like(labels, dtype=float)
    else:
        probabilities = np.asarray(probabilities)

    # -1 = noise, azt külön hagyjuk ki
    clusters = sorted([c for c in np.unique(labels) if c != -1])
    rows = []

    rng = np.random.default_rng(42)

    for c in clusters:
        idx = np.where(labels == c)[0]
        n = len(idx)
        if n < 2:
            # 1 elemű klaszter: távolságok 0
            rows.append({
                "cluster": c,
                "n": n,
                "mean_dist_to_centroid": 0.0,
                "mean_pairwise_dist": 0.0,
                "diameter": 0.0,
                "mean_dist_to_medoid": 0.0,
                "weighted_mean_dist_to_centroid": 0.0
            })
            continue

        Xi = X[idx]
        wi = probabilities[idx]
        wi_norm = wi / wi.sum()

        # --- Centroid és centroid-távolságok ---
        centroid = (wi_norm[:, None] * Xi).sum(axis=0)  # súlyozott centroid; ha nem akarsz súlyt: Xi.mean(axis=0)
        d_to_centroid = np.linalg.norm(Xi - centroid, axis=1) if metric == "euclidean" \
                        else pairwise_distances(Xi, centroid.reshape(1, -1), metric=metric).ravel()

        mean_dist_to_centroid = d_to_centroid.mean()
        weighted_mean_dist_to_centroid = np.average(d_to_centroid, weights=wi)

        # --- Pár-távolságok (átlag és átmérő) ---
        # Nagy klaszternél mintavételezzünk a kvadratikus költség miatt
        if n > 1_500:
            samp_idx = rng.choice(n, size=min(n, int(np.sqrt(sample_cap))), replace=False)
            D = pairwise_distances(Xi[samp_idx], metric=metric)
        else:
            D = pairwise_distances(Xi, metric=metric)

        # csak felső háromszög (i<j) a párokhoz
        iu = np.triu_indices_from(D, k=1)
        pair_vals = D[iu]
        mean_pairwise = pair_vals.mean()
        diameter = pair_vals.max() if pair_vals.size > 0 else 0.0

        # --- Medoid (robosztus „középpont”) és medoid távolságok ---
        # medoid = az a pont, amely minimális átlagos távolságra van a többiektől
        if D.shape[0] == n:
            # teljes D
            avg_row = D.mean(axis=1)
            medoid_idx_local = int(np.argmin(avg_row))
            medoid = Xi[medoid_idx_local]
            d_to_medoid = D[medoid_idx_local]
        else:
            # minta alapján becslés a medoidra
            avg_row = D.mean(axis=1)
            medoid_est = Xi[samp_idx[np.argmin(avg_row)]]
            d_to_medoid = pairwise_distances(Xi, medoid_est.reshape(1, -1), metric=metric).ravel()

        mean_dist_to_medoid = d_to_medoid.mean()

        rows.append({
            "cluster": c,
            "n": n,
            "mean_dist_to_centroid": float(mean_dist_to_centroid),
            "weighted_mean_dist_to_centroid": float(weighted_mean_dist_to_centroid),
            "mean_pairwise_dist": float(mean_pairwise),
            "diameter": float(diameter),
            "mean_dist_to_medoid": float(mean_dist_to_medoid),
        })

    return pd.DataFrame(rows).sort_values(["n", "cluster"], ascending=[False, True])
