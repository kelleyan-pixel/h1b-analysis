import pandas as pd
import glob
import numpy as np
import re
import io

#DATA CLEANING AND PROCESSING

files = glob.glob("../data/*.parquet")

print(f"Number of files: {len(files)}")

df_list = []
i = 0

rate_to_mult = {"Hour":40*52, "Month":12, "Week":52,"Bi-Weekly":26,"Year":1}

def clean_wage(value):
    if pd.isna(value):
        return np.nan
    
    value = str(value)
    value = value.replace(',', '')
    
    if '-' in value:
        parts = re.findall(r'\d+\.?\d*', value)
        if len(parts) >= 2:
            return np.mean([float(parts[0]), float(parts[1])])
    
    match = re.search(r'\d+\.?\d*', value)
    if match:
        return float(match.group())
    
    return np.nan

def clean_dates(df):
    for col in ['CASE_SUBMITTED','EMPLOYMENT_START_DATE']:
        # only try columns that might be dates
        s = (df[col].astype(str).str.strip().str.split(' ').str[0])
        s = s.replace(['', 'nan', 'None'], np.nan)
        parsed = pd.to_datetime(s,errors='coerce')
        if parsed.notna().sum()/len(df) <0.1:  # if less than 10% parsed, try manual formats
            print(f"Retrying {col} with manual formats...")
            formats = ['%m/%d/%Y',
                        '%Y-%m-%d',
                        '%m-%d-%Y'
                    ]
            
            for fmt in formats:
                parsed = pd.to_datetime(s, format=fmt, errors='coerce')
                if parsed.notna().sum() > 0:
                    break

        mask = parsed.isna()
        if mask.any():
            parsed2 = pd.to_datetime(
                s[mask],
                format='%d-%b-%y',
                errors='coerce'
            )
            parsed.loc[mask] = parsed2
        
        df[col] = parsed
        
        print(f"{col}: parsed {df[col].notna().sum()} values")
    
    return df

def clean_employer_name(val):
    if pd.isna(val):
        return None
    
    try:
        # convert to string safely
        val = str(val)
        
        # lowercase
        val = val.lower()
        
        # remove accents / weird chars safely
        val = val.encode('ascii', 'ignore').decode('ascii')
        
        # remove punctuation
        val = re.sub(r'[^a-z0-9\s]', ' ', val)
        
        # remove common suffixes
        suffixes = [
            'inc', 'incorporated', 'llc', 'l l c', 'ltd', 'limited',
            'corp', 'corporation', 'co', 'company', 'plc'
        ]
        
        words = val.split()
        words = [w for w in words if w not in suffixes]
        
        # remove common filler words (optional but helpful)
        fillers = ['the', 'of', 'and']
        words = [w for w in words if w not in fillers]
        
        # collapse whitespace
        val = " ".join(words)
        
        return val.strip()
    
    except:
        return None


for f in files:
    df = pd.read_parquet(f)

    print(f"\nDataset {i} initial:", df.shape)

    #Column renaming to standardize across datasets
    rename_map = {
        'TOTAL WORKERS':'TOTAL_WORKER_POSITIONS','NAIC_CODE':'NAICS_CODE','STATUS':'CASE_STATUS',
        'LCA_CASE_EMPLOYER_CITY':'EMPLOYER_CITY','LCA_CASE_EMPLOYER_STATE':'EMPLOYER_STATE',
        'LCA_CASE_SOC_CODE':'SOC_CODE','LCA_CASE_SOC_NAME':'SOC_TITLE','LCA_CASE_JOB_TITLE':'JOB_TITLE',
        'LCA_CASE_WAGE_RATE_FROM':'WAGE_RATE_OF_PAY_FROM','LCA_CASE_WAGE_RATE_TO':'WAGE_RATE_OF_PAY_TO',
        'LCA_CASE_WAGE_RATE_UNIT':'WAGE_UNIT_OF_PAY','LCA_CASE_WORKLOC1_CITY':'WORKSITE_CITY',
        'LCA_CASE_WORKLOC1_STATE':'WORKSITE_STATE','PW_1':'PREVAILING_WAGE','PW_UNIT_1':'PW_UNIT_OF_PAY',
        'LCA_CASE_NAICS_CODE':"NAICS_CODE","FULL_TIME_POS":"FULL_TIME_POSITION",
        "PW_UNIT_OF_PAY_1":"PW_UNIT_OF_PAY","PREVAILING_WAGE_1":"PREVAILING_WAGE",
        "WAGE_RATE_OF_PAY_FROM_1":"WAGE_RATE_OF_PAY_FROM",
        "WAGE_RATE_OF_PAY_TO_1":"WAGE_RATE_OF_PAY_TO","WAGE_UNIT_OF_PAY_1":"WAGE_UNIT_OF_PAY",
        "RECEIVED_DATE":"CASE_SUBMITTED","BEGIN_DATE":"EMPLOYMENT_START_DATE", "LCA_CASE_NUMBER":"CASE_NUMBER",
        "LCA_CASE_EMPLOYER_NAME":"EMPLOYER_NAME","LCA_CASE_SOC_TITLE":"SOC_TITLE","SOC_NAME":"SOC_TITLE",
        "WORKSITE_CITY_1":"WORKSITE_CITY","WORKSITE_COUNTY_1":"WORKSITE_COUNTY","WORKSITE_STATE_1":"WORKSITE_STATE",
        "TOTAL_WORKERS":"TOTAL_WORKER_POSITIONS","WORKSITE_WORKERS_1":"WORKSITE_WORKERS","WILLFUL_VIOLATOR_1":"WILLFUL_VIOLATOR",
        "WILLFUL VIOLATOR":"WILLFUL_VIOLATOR", "LCA_CASE_SUBMITTED":"CASE_SUBMITTED", "LCA_CASE_EMPLOYER_CITY":"EMPLOYER_CITY",
        "LCA_CASE_EMPLOYMENT_START_DATE":"EMPLOYMENT_START_DATE", "LCA_CASE_EMPLOYER_NAME":"EMPLOYER_NAME",
        "WORK_LOCATION_CITY1":"WORKSITE_CITY", "WORK_LOCATION_STATE1":"WORKSITE_STATE", "WORK_LOCATION_COUNTY1":"WORKSITE_COUNTY",
        "LCA_CASE_SUBMIT":"CASE_SUBMITTED",'PERIOD_OF_EMPLOYMENT_START_DATE':"EMPLOYMENT_START_DATE"
    }

    df = df.rename(columns={k: v for k, v in rename_map.items() if k in df.columns})
    #Filters
    if 'CASE_STATUS' in df.columns:
        df = df[df['CASE_STATUS'].isin(["CERTIFIED","Certified"])]

    if 'VISA_CLASS' in df.columns:
        df = df[df['VISA_CLASS']=='H-1B']

    if 'FULL_TIME_POSITION' in df.columns:
        df = df[df['FULL_TIME_POSITION']=="Y"]

    #Clean wage columns
    if 'WAGE_RATE_OF_PAY_FROM' in df.columns:
        wage_from = df['WAGE_RATE_OF_PAY_FROM'].apply(clean_wage)
        wage_to = df['WAGE_RATE_OF_PAY_TO'].apply(clean_wage)

        df['WAGE_RATE_OF_PAY'] = pd.concat([wage_from, wage_to], axis=1).mean(axis=1)

    elif 'WAGE_RATE_OF_PAY' in df.columns:
        df['WAGE_RATE_OF_PAY'] = df['WAGE_RATE_OF_PAY'].apply(clean_wage)

    else:
        print("NO WAGE COLUMN → skipping dataset")
        i += 1
        continue

    df['PREVAILING_WAGE'] = df['PREVAILING_WAGE'].apply(clean_wage)

    # multiplier
    if 'WAGE_UNIT_OF_PAY' in df.columns:
        mult = df['WAGE_UNIT_OF_PAY'].map(rate_to_mult)
    else:
        mult = 1

    if isinstance(mult, pd.Series):
        mult = mult.fillna(1)

    df['WAGE_RATE_OF_PAY'] = df['WAGE_RATE_OF_PAY'] * mult

    if 'PW_UNIT_OF_PAY' in df.columns:
        mult = df['PW_UNIT_OF_PAY'].map(rate_to_mult)
    else:
        mult = 1

    if isinstance(mult, pd.Series):
        mult = mult.fillna(1)

    df['PREVAILING_WAGE'] = df['PREVAILING_WAGE'] * mult

    # drop missing
    df = df[df['WAGE_RATE_OF_PAY'].notna()]

    #Filter to relevant columns
    cols_to_keep = [
        'CASE_NUMBER','CASE_SUBMITTED','EMPLOYMENT_START_DATE','EMPLOYER_NAME','EMPLOYER_CITY','EMPLOYER_STATE',
        'JOB_TITLE','SOC_CODE','SOC_NAME','PREVAILING_WAGE','WAGE_RATE_OF_PAY',
        'WILLFUL_VIOLATOR','WORKSITE_CITY','WORKSITE_COUNTY','WORKSITE_STATE','SOC_TITLE',
        'NAICS_CODE'
    ]

    df = df[[col for col in cols_to_keep if col in df.columns]]
    df['dataset'] = i
    df = clean_dates(df)

    print("Final dataset shape:", df.shape)

    if len(df) > 0:
        df_list.append(df)
    else:
        print("Dataset", i, "EMPTY — skipped")

    print(i, df.PREVAILING_WAGE.isna().sum()/len(df), "missing prevailing wage")

    i += 1


df = pd.concat(df_list, ignore_index=True)
df['NAICS_CODE'] = pd.to_numeric(df['NAICS_CODE'], errors='coerce')
df.drop_duplicates(subset=['CASE_NUMBER'], inplace=True)

df['CASE_SUBMITTED'] = pd.to_datetime(df['CASE_SUBMITTED'], errors='coerce')
df['EMPLOYMENT_START_DATE'] = pd.to_datetime(df['EMPLOYMENT_START_DATE'], errors='coerce')
df['year'] = df['CASE_SUBMITTED'].dt.year
df['month'] = df['CASE_SUBMITTED'].dt.month

df.to_parquet("../data/final_cleaned.parquet", index=False)
df = pd.read_parquet("../data/final_cleaned.parquet", engine="fastparquet")  # re-read to ensure clean types

print("\nFINAL SHAPE:", df.shape)

df['EMPLOYER_NAME_CLEAN'] = df['EMPLOYER_NAME'].apply(clean_employer_name)

df.to_parquet("../data/final_cleaned.parquet", index=False)
