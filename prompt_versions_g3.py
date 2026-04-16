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
            #"The calculation usually involves two steps: a selection and a calculation on selected data. "+
            "If the question is about calculating the year average, you must calculate the average between the given year and the previous one. ex. 2015_average = (2015_value + 2014_value)/2  "+
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
     'V19b': [ #0.21428571428571427 cluster delta
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
            "During the average calculation, usually, there is only one data item per year.  "+
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
     'V19c': [
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
            "If the question is about calculating the year average, you must calculate the average between the given year and the previous one.  "+
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
      'V19d': [
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
            "Average of years ex.: (2015_value + 2014_value)/2"+
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
       'V19e': [
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
            "Average of years: (year1 value + year2 value)/2 "+
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
      ] ,
       'V19f': [
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
            "Average of years: (year1 value + year2 value)/2 "+
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
    'V20a': [
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
            "If the question is about calculating the year average, you must calculate the average between the given year and the previous one. ex. 2015_average = (2015_value + 2014_value)/2  "+
            "Thousand scale is usually indicated in value list. " + 
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
     'V20b': [
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
            "If the question is about calculating the year average, you must calculate the average between the given year and the previous one. ex. 2015_average = (2015_value + 2014_value)/2  "+
            "Result scale is thousand if indicated in value list. " + 
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
          "The function must return a tuple (number, scale). The resulting number is a float with accuracy to two decimal places. Scale usually is thousand, million, billion, percent or an empty string. "+
            #"The calculation usually involves two steps: a selection and a calculation on selected data. "+
            "If the question is about calculating the year average, you must calculate the average between the given year and the previous one. ex. 2015_average = (2015_value + 2014_value)/2  "+
            "If the question is about calculating average between years each year has only one value.  " + 
            #"Result scale is thousand if indicated in value list. " + 
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

     'V22': [
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
            #"Scale is thousand if indicated in value list. " + #0.22
            #"; scale is thousand if indicated in value list. " + #0.13
            "Average of years ex.: (2015_value + 2014_value)/2"+      
            "; scale is thousand if indicated in value list. " + #0.33
            #"; scale is thousand if it is in the value list. " + #0.1944
            #"; thousand scale is indicated in value list. " +  #0.08
            #"; scale is thousand only if indicated in value list. " + #0.16
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
     'V23': [
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
            #"Scale is thousand if indicated in value list. " + #0.22
            #"; scale is thousand if indicated in value list. " + #0.13
            "Average of years ex.: (2015_value + 2014_value)/2 "+      
            "'percentage change' has formula '(a-b)/b' and results 'percent' scale;" +
            #"; scale is thousand if indicated in value list. " + #0.33
            #"; scale is thousand if it is in the value list. " + #0.1944
            #"; thousand scale is indicated in value list. " +  #0.08
            #"; scale is thousand only if indicated in value list. " + #0.16
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
     'V23b': [
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
            #"Scale is thousand if indicated in value list. " + #0.22
            #"; scale is thousand if indicated in value list. " + #0.13
            "Average of years ex.: (2015_value + 2014_value)/2; "+      
            "'percentage change' has formula '(a-b)/b' and results 'percent' scale; " +
            #"; scale is thousand if indicated in value list. " + #0.33
            #"; scale is thousand if it is in the value list. " + #0.1944
            #"; thousand scale is indicated in value list. " +  #0.08
            #"; scale is thousand only if indicated in value list. " + #0.16
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
}