import pandas as pd
import re

###############################################################################
# STEP 1: Read the final filled data from Excel
###############################################################################
df_filled = pd.read_excel("FDI_data_filled.xlsx")  # update file name/path as needed

###############################################################################
# STEP 2: Reconstruct the hierarchy (rows list with parent links)
#
# This block is essentially your original hierarchy‐parsing code.
###############################################################################
def parse_category(cat_str):
    cat_str = cat_str.strip()
    if cat_str.upper() == "TOTAL":
        return -1, None, None
    if not re.search(r'\d', cat_str):
        return 0, None, None
    # Special pattern: hyphenated, e.g. "31-33"
    m_hyphen = re.match(r'^(\d{2})\s*-\s*(\d{2})', cat_str)
    if m_hyphen:
        start = int(m_hyphen.group(1))
        end = int(m_hyphen.group(2))
        special_range = {f"{num:02d}" for num in range(start, end + 1)}
        return 2, None, special_range
    # Special pattern: with "y", e.g. "43 y 46"
    m_y = re.match(r'^(\d{2})\s*y\s*(\d{2})', cat_str, re.IGNORECASE)
    if m_y:
        special_range = {m_y.group(1), m_y.group(2)}
        return 2, None, special_range
    m_digits = re.match(r'^(\d+)', cat_str)
    if m_digits:
        digits = m_digits.group(1)
        if len(digits) == 2:
            return 2, digits, None
        elif len(digits) == 3:
            return 3, digits, None
        elif len(digits) >= 4:
            return 4, digits[:4], None
    return 0, None, None

last_seen = {}       # to track most recent row index for each level
rows = []            # list of row dictionaries
children_map = {}    # mapping: parent row index -> list of child row indices

for i in range(len(df_filled)):
    raw_cat = df_filled.at[i, "Category"]
    cat_str = str(raw_cat)
    level, code, special_range = parse_category(cat_str)
    
    row_dict = {
        "index": i,
        "name": cat_str.strip(),
        "level": level,
        "code": code,                   # extracted code, if any
        "special_range": special_range, # special range, if any
        "parent": None,
        "values": { col: df_filled.at[i, col] for col in df_filled.columns if col != "Category" }
    }
    rows.append(row_dict)
    
    if level == -1:
        last_seen[-1] = i
    elif level == 0:
        if -1 in last_seen:
            row_dict["parent"] = last_seen[-1]
            children_map.setdefault(last_seen[-1], []).append(i)
        last_seen[0] = i
    elif level == 2:
        if 0 in last_seen:
            row_dict["parent"] = last_seen[0]
            children_map.setdefault(last_seen[0], []).append(i)
        last_seen[2] = i
    elif level == 3:
        if 2 in last_seen:
            candidate = rows[last_seen[2]]
            if candidate.get("special_range"):
                if code and code[:2] in candidate["special_range"]:
                    row_dict["parent"] = last_seen[2]
                    children_map.setdefault(last_seen[2], []).append(i)
            else:
                parent_code = candidate.get("code")
                if parent_code and code and code.startswith(parent_code):
                    row_dict["parent"] = last_seen[2]
                    children_map.setdefault(last_seen[2], []).append(i)
        last_seen[3] = i
    elif level == 4:
        if 3 in last_seen:
            candidate = rows[last_seen[3]]
            parent_code = candidate.get("code")
            if parent_code and code and code.startswith(parent_code):
                row_dict["parent"] = last_seen[3]
                children_map.setdefault(last_seen[3], []).append(i)
        last_seen[4] = i
    else:
        pass

###############################################################################
# STEP 3: Helper functions for reformatting
###############################################################################
def get_top_level_parent(index_to_row, row):
    """
    Climb the parent chain until reaching a row with level 0.
    If no level 0 parent is found, return row's own name (as fallback).
    """
    if row["level"] == 0:
        return row["name"]
    while row["parent"] is not None:
        row = index_to_row[row["parent"]]
        if row["level"] == 0:
            return row["name"]
    # Fallback: if no parent is found, return the row's own name.
    return row["name"]

def parse_sector_code_and_name(category_text):
    text = str(category_text).strip()
    m_hyphen = re.match(r'^(\d{2}\s*-\s*\d{2})(.*)$', text)
    if m_hyphen:
        return m_hyphen.group(1).strip(), m_hyphen.group(2).strip()
    m_y = re.match(r'^(\d{2}\s*y\s*\d{2})(.*)$', text, re.IGNORECASE)
    if m_y:
        return m_y.group(1).strip(), m_y.group(2).strip()
    m_digits = re.match(r'^(\d{1,4})(.*)$', text)
    if m_digits:
        return m_digits.group(1).strip(), m_digits.group(2).strip()
    return "", text

def convert_numeric_col_name(col):
    m1 = re.match(r'^(\d{4})_(\d+)$', col)
    if m1:
        year = m1.group(1)
        season = m1.group(2)
        return f"FDI_{year}Q{season}"
    m2 = re.match(r'^(?i:total)\s*(\d{4})$', col)
    if m2:
        year = m2.group(1)
        return f"FDI_{year}"
    return col

def reformat_fdi_data(df_filled, rows):
    index_to_row = { r["index"]: r for r in rows }
    all_cols = list(df_filled.columns)
    numeric_cols = [c for c in all_cols if c != "Category"]
    numeric_cols_new = [convert_numeric_col_name(c) for c in numeric_cols]
    
    out_records = []
    for rd in rows:
        lvl = rd["level"]
        cat_text = rd["name"]
        # For each row, determine the country by climbing up to a level 0 parent.
        country = get_top_level_parent(index_to_row, rd)
        if lvl == 0:
            sector_code, sector_name = "", ""
        else:
            sector_code, sector_name = parse_sector_code_and_name(cat_text)
        record = {
            "country": country,
            "sector_code": sector_code,
            "sector_name": sector_name
        }
        for old_col, new_col in zip(numeric_cols, numeric_cols_new):
            record[new_col] = rd["values"].get(old_col, None)
        out_records.append(record)
    
    df_final = pd.DataFrame(out_records)
    col_order = ["country", "sector_code", "sector_name"] + numeric_cols_new
    df_final = df_final[col_order]
    return df_final

###############################################################################
# STEP 4: Reformat the data and save final DataFrame
###############################################################################
df_final = reformat_fdi_data(df_filled, rows)
df_final.to_excel("FDI_reformatted.xlsx", index=False)
print("Reformat done. Final shape:", df_final.shape)
