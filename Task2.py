# Task 2

from pprint import pprint

class Title_Mapper:
    """
        Title_Mapper is a class used to clean up a list of company titles
    """
    
    # ******** global variables ********
    # Default hashmap for company mapping
    default_companies_mapping = {"Saama Technologies": "Saama Technologies",
                                "SaamaTech, Inc.": "Saama Technologies",
                                "Takeda Pharmaceutical": "Takeda Pharmaceutical",
                                "AstraZeneca": "AstraZeneca"
                                }
    
    
    # ******************************************************
    # Constructor for Title_Mapper
    # ******************************************************
    
    def __init__(self, companies_mapping = default_companies_mapping):
        """
            :param companies_mapping: a hashmap mapping title variations to preferred titles. 
                                    if not set, default_companies_mapping is set
        """
        
        self.companies_mapping = companies_mapping
        

    # ******************************************************
    # Lematize
    # ******************************************************
    
    def lematize(self, title):
        """
        Pre-processes title by lemmatizing text and returning proper title.
        :param name: a string containing the company name.
        :return: processed company title.
        """

        # For each predefined companies, find the close match to the predefined company titles.
        for company in self.companies_mapping:
            if company.upper() in title.upper():
                return self.companies_mapping[company]

        # Return string if no match is found
        return title

    # ******************************************************
    # lematize_list
    # ******************************************************
    
    def lematize_list(self, title_list):
        """
        Pre-processes a list of titles by calling lematize().
        :param name: a list containing company names.
        :return: processed list of titles.
        """

        processed_list = []

        # For every title, find the close match to the predefined company titles.
        for title in title_list:
            processed_list.append(self.lematize(title))

        return processed_list

    
# ******************************************************    
# ************** DRIVER PROGRAM *********************** 
# ******************************************************

if __name__ == "__main__":   
    t_mapper = Title_Mapper()

    input_list = ["Equipment ONLY - Saama Technologies",
                "Saama Technologies",
                "SaamaTech, Inc.",
                "Takeda Pharmaceutical SA - Central Office",
                "*** DO NOT USE *** Takeda Pharmaceutical",
                "Takeda Pharmaceutical, SA",
                "Ship to AstraZeneca",
                "AstraZeneca, gmbh Munich",
                "AstraZeneca (use AstraZeneca, gmbh Munich acct 84719482-A)"]

    
    print("*******************************")
    print("******* TEST CASE I ***********")
    
    processed_result = t_mapper.lematize_list(input_list)
    
    print("\n-----------------")
    print("Input List:")
    print("-----------------")
    pprint(input_list)
    
    print("\n-----------------")
    print("Output List:")
    print("-----------------")
    pprint(processed_result)
    
    
    print("\n*******************************")
    print("******* TEST CASE II ***********")
    
    # Creating a custom company mapping
    new_company_mapping = {"Saama Technologies": "Saama Tech",
                            "SaamaTech, Inc.": "Saama Tech",
                            "Takeda Pharmaceutical": "Takeda Pharma",
                            "AstraZeneca": "AstraZeneca Inc."
                            }
    
    # Creating a new title mapper that instantiaties with a custom company mapping
    t_mapper = Title_Mapper(new_company_mapping)
    
    # Calling the mapper to lematize/preprocess the titles
    processed_result = t_mapper.lematize_list(input_list)
    
    print("\n-----------------")
    print("Input List:")
    print("-----------------")
    pprint(input_list)
    
    print("\n-----------------")
    print("Output List:")
    print("-----------------")
    pprint(processed_result)
    
"""
*******************************
******* TEST CASE I ***********

-----------------
Input List:
-----------------
['Equipment ONLY - Saama Technologies',
 'Saama Technologies',
 'SaamaTech, Inc.',
 'Takeda Pharmaceutical SA - Central Office',
 '*** DO NOT USE *** Takeda Pharmaceutical',
 'Takeda Pharmaceutical, SA',
 'Ship to AstraZeneca',
 'AstraZeneca, gmbh Munich',
 'AstraZeneca (use AstraZeneca, gmbh Munich acct 84719482-A)']

-----------------
Output List:
-----------------
['Saama Technologies',
 'Saama Technologies',
 'Saama Technologies',
 'Takeda Pharmaceutical',
 'Takeda Pharmaceutical',
 'Takeda Pharmaceutical',
 'AstraZeneca',
 'AstraZeneca',
 'AstraZeneca']

*******************************
******* TEST CASE II ***********

-----------------
Input List:
-----------------
['Equipment ONLY - Saama Technologies',
 'Saama Technologies',
 'SaamaTech, Inc.',
 'Takeda Pharmaceutical SA - Central Office',
 '*** DO NOT USE *** Takeda Pharmaceutical',
 'Takeda Pharmaceutical, SA',
 'Ship to AstraZeneca',
 'AstraZeneca, gmbh Munich',
 'AstraZeneca (use AstraZeneca, gmbh Munich acct 84719482-A)']

-----------------
Output List:
-----------------
['Saama Tech',
 'Saama Tech',
 'Saama Tech',
 'Takeda Pharma',
 'Takeda Pharma',
 'Takeda Pharma',
 'AstraZeneca Inc.',
 'AstraZeneca Inc.',
 'AstraZeneca Inc.']

"""
