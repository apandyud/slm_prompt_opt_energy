prompt_versions = {
    'V18': [
         ("system","You will receive the financial report as an annotated value list and the question. "+
             "Your task is to generate a Python function that can calculate a numeric value that is the answer for the received question. "                            
         ),
        (
          "human",
          "VALUE_LIST: {value_list}"+
          "QUESTION: {question}" +
          "Generate a Python function 'run(value_list)' that can answer the question using the list of annotated values! "+
          "The function must return a tuple (number, scale). The resulting number is a float with accuracy to two decimal places. Scale usually is thousand, million, billion, percent or an empty string. "+
            #"The calculation usually involves two steps: a selection and a calculation on selected data. "+
            #"If the question is about calculating the year average, you must calculate the average between the given year and the previous one. ex. 2015_average = (2015_value + 2014_value)/2  "+
            #"If the question is about calculating the change between year averages, apply the previous logic and take difference. " +            
            #"Expenses are revenue minus net income. " +            
            #"To calculate the difference, use absolute value. " +
            #"To calculate the difference, subtract from the bigger number. " +
            #"To calculate difference, use equation: difference = abs(value1 - value2)." +
            #"To calculate change, use equation: change = (2*signed_new_value - 2*signed_old_value)/2. "+
            #"To calculate change, use equation: change = (new_value - old_value)/2. "+
            #"To calculate percentage change, use equation: percentage_change = (2*signed_new_value - 2*signed_old_value)/2*signed_old_value * 100. " +
            #"To calculate proportion, do NOT multiply by 100. " +
            #"Use all given year values if no year specified. " +
            #"The code must be specific to the provided value list. " +
            "Do not generate explanation, nor example code, just the function. "           
        )        
      ],

    'V19a': [
         ("system","You will receive the financial report as an annotated value list and the question. "+
             "Your task is to generate a Python function that can calculate a numeric value that is the answer for the received question. "                            
         ),
        (
          "human",
          "VALUE_LIST: {value_list}"+
          "QUESTION: {question}" +
          "Generate a Python function 'run(value_list)' that can answer the question using the list of annotated values! "+
          "The function must return a tuple (number, scale). The resulting number is a float with accuracy to two decimal places. Scale usually is thousand, million, billion, percent or an empty string. "+
            "'percentage change' results 'percent' scale; " +
            #"The calculation usually involves two steps: a selection and a calculation on selected data. "+
            #"If the question is about calculating the year average, you must calculate the average between the given year and the previous one. ex. 2015_average = (2015_value + 2014_value)/2  "+
            #"If the question is about calculating the change between year averages, apply the previous logic and take difference. " +            
            #"Expenses are revenue minus net income. " +            
            #"To calculate the difference, use absolute value. " +
            #"To calculate the difference, subtract from the bigger number. " +
            #"To calculate difference, use equation: difference = abs(value1 - value2)." +
            #"To calculate change, use equation: change = (2*signed_new_value - 2*signed_old_value)/2. "+
            #"To calculate change, use equation: change = (new_value - old_value)/2. "+
            #"To calculate percentage change, use equation: percentage_change = (2*signed_new_value - 2*signed_old_value)/2*signed_old_value * 100. " +
            #"To calculate proportion, do NOT multiply by 100. " +
            #"Use all given year values if no year specified. " +
            #"The code must be specific to the provided value list. " +
            "Do not generate explanation, nor example code, just the function. "           
        )        
      ],
      'V20': [
         ("system","You will receive the financial report as an annotated value list and the question. "+
             "Your task is to generate a Python function that can calculate a numeric value that is the answer for the received question. "                            
         ),
        (
          "human",
          "VALUE_LIST: {value_list}"+
          "QUESTION: {question}" +
          "Generate a Python function 'run(value_list)' that can answer the question using the list of annotated values! "+
          "The function must return a tuple (number, scale). The resulting number is a float with accuracy to two decimal places. Scale usually is thousand, million, billion, percent or an empty string. "+
            "'percentage change' results 'percent' scale; " +
            #"Use string literals in code exactly as they are in the VALUE_LIST; "
            #"Do NOT improve wording. Do NOT change capitalization. "+
            "Do NOT reformat snake_case or camelCase " +
            #"All dictionary keys and string values must be treated as opaque tokens. Copy them character-by-character from the input. " +
            #"Before finishing: Verify that every dictionary key and string literal matches exactly one that appears in the input.; " +
            #"The calculation usually involves two steps: a selection and a calculation on selected data. "+
            #"If the question is about calculating the year average, you must calculate the average between the given year and the previous one. ex. 2015_average = (2015_value + 2014_value)/2  "+
            #"If the question is about calculating the change between year averages, apply the previous logic and take difference. " +            
            #"Expenses are revenue minus net income. " +            
            #"To calculate the difference, use absolute value. " +
            #"To calculate the difference, subtract from the bigger number. " +
            #"To calculate difference, use equation: difference = abs(value1 - value2)." +
            #"To calculate change, use equation: change = (2*signed_new_value - 2*signed_old_value)/2. "+
            #"To calculate change, use equation: change = (new_value - old_value)/2. "+
            #"To calculate percentage change, use equation: percentage_change = (2*signed_new_value - 2*signed_old_value)/2*signed_old_value * 100. " +
            #"To calculate proportion, do NOT multiply by 100. " +
            #"Use all given year values if no year specified. " +
            #"The code must be specific to the provided value list. " +
            "Do not generate explanation, nor example code, just the function. "           
        )        
      ],
     'V21': [
         ("system","You will receive the financial report as an annotated value list and the question. "+
             "Your task is to generate a Python function that can calculate a numeric value that is the answer for the received question. "                            
         ),
        (
          "human",
          "VALUE_LIST: {value_list}"+
          "QUESTION: {question}" +
          "Generate a Python function 'run(value_list)' that can answer the question using the list of annotated values! "+
          "The function must return a tuple (number, scale). The resulting number is a float with accuracy to two decimal places. Scale is 'thousand', 'million', 'billion', 'percent' or an empty string. "+
            "'percentage change' results 'percent' scale; " +           
            "Do not generate explanation, nor example code, just the function. "           
        )        
      ],
    'V22': [
         ("system","You will receive the financial report as an annotated value list and the question. "+
             "Your task is to generate a Python function that can calculate a numeric value that is the answer for the received question. "                            
         ),
        (
          "human",
          "VALUE_LIST: {value_list}"+
          "QUESTION: {question}" +
          "Generate a Python function 'run(value_list)' that can answer the question using the list of annotated values! "+
          "The function must return a tuple (number, scale). The resulting number is a float with accuracy to two decimal places. Scale is 'thousand', 'million', 'billion', 'percent' or an empty string. "+
            "'percentage change' results 'percent' scale; " +           
            
            "Use string literals in code exactly as they are in the VALUE_LIST; "
            "Do NOT improve wording. Do NOT change capitalization. "+
            "All dictionary keys and string values must be treated as opaque tokens. Copy them character-by-character from the input. " +
            "Before finishing: Verify that every dictionary key and string literal matches exactly one that appears in the input.; " +
          
            "Do not generate explanation, nor example code, just the function. "           
        )        
      ],
    'V22b': [
         ("system","You will receive the financial report as an annotated value list and the question. "+
             "Your task is to generate a Python function that can calculate a numeric value that is the answer for the received question. "                            
         ),
        (
          "human",
          "VALUE_LIST: {value_list}"+
          "QUESTION: {question}" +
          "Generate a Python function 'run(value_list)' that can answer the question using the list of annotated values! "+
          "The function must return a tuple (number, scale). The resulting number is a float with accuracy to two decimal places. Scale is 'thousand', 'million', 'billion', 'percent' or an empty string. "+
            "'percentage change' results 'percent' scale; " +           
            
              "CRITICAL INSTRUCTIONS: - You must extract the field values EXACTLY as they appear in the dictionaries. - Do NOT alter, correct, or misspell the values. Copy them character-by-character. " +
              
            "Do not generate explanation, nor example code, just the function. "           
        )        
      ],

     'V23': [
         ("system","You will receive the financial report as an annotated value list and the question. "+
             "Your task is to generate a Python function that can calculate a numeric value that is the answer for the received question. "                            
         ),
        (
          "human",
          "VALUE_LIST: {value_list}"+
          "QUESTION: {question}" +
          "Generate a Python function 'run(value_list)' that can answer the question using the list of annotated values! "+
          "The function must return a tuple (number, scale). The resulting number is a float with accuracy to two decimal places. Scale is 'thousand', 'million', 'billion', 'percent' or an empty string. "+
            "'percentage change' results 'percent' scale; " +           
            "'category' value MUST be used in calculation; "+
            "Do not generate explanation, nor example code, just the function. "           
        )        
      ],
}