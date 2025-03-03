import pandas as pd
import re
from openpyxl import load_workbook

##############################################################################
# USER PARAMETERS
##############################################################################
filename = "inversion.xlsx"        # the input Excel file
output_filename = "inversion_reformatted.xlsx"

# The three known line items for level 1 (case-insensitive):
level1_map = {
    "NUEVAS": "N_",         # e.g. "Nuevas inversiones"
    "REINVERS": "V_",       # e.g. "Reinversión de utilidades"
    "CUENTAS": "C_"         # e.g. "Cuentas entre compañías"
}

##############################################################################
# STEP 1: We no longer use bold to detect level 0.
#         We simply read the first column's text and see if it matches
#         one of the 3 known items => level 1, else => level 0
##############################################################################
wb = load_workbook(filename, data_only=True)
ws = wb.active

rows_list = []
for row_cells in ws.iter_rows(min_row=3, max_col=1):
    cell = row_cells[0]
    cat_text = str(cell.value).strip() if cell.value else ""
    cat_up = cat_text.upper()

    level = 0  # default
    # If cat_up contains one of the known items => level=1
    # e.g. if "NUEVAS" in cat_up => level=1, prefix=N_
    # But we won't pick prefix now, just do level=1
    if any(kword in cat_up for kword in level1_map.keys()):
        level = 1
    
    rows_list.append({
        "excel_row": cell.row,
        "category": cat_text,
        "level": level
    })

print(f"Openpyxl read {len(rows_list)} data rows starting row 3.")

##############################################################################
# STEP 2: Read the same file with pandas, using header=[0,1] for 2 header rows
##############################################################################
df = pd.read_excel(filename, header=[0,1])
df.reset_index(drop=True, inplace=True)

print(f"Pandas read {len(df)} data rows total.")

# If mismatch => trim
min_len = min(len(df), len(rows_list))
if len(df)!=len(rows_list):
    print(f"Warning: pandas has {len(df)} rows, openpyxl has {len(rows_list)} rows. Trunc => {min_len}.")
df = df.iloc[:min_len].copy()
rows_list = rows_list[:min_len]

##############################################################################
# STEP 3: Flatten the MultiIndex columns => e.g. "1999Q1" or "1999"
##############################################################################
def flatten_col(col_pair):
    """
    (year, season) => e.g. ('1999','1') => '1999Q1'
    ('1999','Total') => '1999'
    """
    year_str = str(col_pair[0]).strip()
    season_str = str(col_pair[1]).strip() if len(col_pair)>1 else ""
    if re.search(r'(?i)total', season_str):
        return year_str
    try:
        s_int = int(float(season_str))
        return f"{year_str}Q{s_int}"
    except:
        if season_str:
            return f"{year_str}{season_str}"
        else:
            return year_str

orig_cols = df.columns.tolist()
new_cols = []
for i, cpair in enumerate(orig_cols):
    if i == 0:
        new_cols.append("TO_DROP")  # presumably old Category col
    else:
        new_cols.append(flatten_col(cpair))

df.columns = new_cols
df.drop(columns=["TO_DROP"], inplace=True, errors="ignore")

##############################################################################
# STEP 4: We expect blocks of 4 rows: 1 level0 (country) + next 3 level1 (N, V, C).
##############################################################################
final_records = []
i = 0
nrows = len(df)

while i<nrows:
    # row i => must be level=0 => country
    if rows_list[i]["level"]!=0:
        print(f"Warning: row {i} not level0 => skipping one row.")
        i +=1
        continue
    country_name = rows_list[i]["category"]
    
    # next 3 => level1 => N, V, C
    if i+3>=nrows:
        print("End of file => partial block => skip.")
        break
    
    # We'll store numeric data in e.g. block_data["N_"][colName] = value
    # so we can combine them
    block_data = {"N_":{}, "V_":{}, "C_":{}}
    valid_block = True

    for j in range(1,4):
        row_idx = i+j
        if rows_list[row_idx]["level"]!=1:
            print(f"Warning: row {row_idx} => not level1 => break block.")
            valid_block=False
            break
        # detect prefix => N_ / V_ / C_
        cat_up = rows_list[row_idx]["category"].upper()
        prefix = None
        # match in level1_map
        for kw,pfix in level1_map.items():
            if kw in cat_up:
                prefix = pfix
                break
        if not prefix:
            print(f"Warning: row {row_idx} text => {cat_up} => no match => skip row.")
            continue
        # gather numeric data from df
        row_dict = df.iloc[row_idx].to_dict()
        for ccc, val in row_dict.items():
            block_data[prefix][ccc] = val

    # if valid_block => produce one final wide row
    if valid_block:
        rec = {}
        rec["Country"] = country_name
        # for each col in df => rec["N_<col>"], rec["V_<col>"], rec["C_<col>"]
        allcols = df.columns
        for coln in allcols:
            rec[f"N_{coln}"] = block_data["N_"].get(coln, None)
            rec[f"V_{coln}"] = block_data["V_"].get(coln, None)
            rec[f"C_{coln}"] = block_data["C_"].get(coln, None)
        final_records.append(rec)

    i += 4  # move to next block

##############################################################################
# STEP 5: Build final DataFrame
##############################################################################
df_final = pd.DataFrame(final_records)
# reorder => Country first
cols = df_final.columns.tolist()
cols.remove("Country")
cols = ["Country"] + cols
df_final = df_final[cols]

##############################################################################
# STEP 6: Save
##############################################################################
df_final.to_excel(output_filename, index=False)
print("Done. Final shape:", df_final.shape)
