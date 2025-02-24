import pandas as pd
import re

###############################################################################
# STEP 1: READ THE EXCEL FILE & RESET THE INDEX
###############################################################################
df = pd.read_excel(
    "FDI.xlsx",                 # <-- update with your file path
    sheet_name="Por sector",    # <-- update sheet name if needed
    header=[0, 1]               # two header rows
)
df.reset_index(drop=True, inplace=True)

###############################################################################
# STEP 2: FLATTEN THE TWO-ROW HEADERS INTO A SINGLE ROW OF COLUMN NAMES
###############################################################################
temp_cols = []
for col_pair in df.columns:
    col_str = "_".join(str(x) for x in col_pair if x)
    temp_cols.append(col_str)
df.columns = temp_cols

###############################################################################
# STEP 3: CLEAN UP "Unnamed:" COLUMNS & RENAME THE FIRST COLUMN TO "Category"
###############################################################################
new_cols = []
for col in df.columns:
    if "Unnamed:" in col:
        parts = col.split("_")
        parts = [p for p in parts if not p.startswith("Unnamed:")]
        col = "_".join(parts)
    new_cols.append(col)
df.columns = new_cols

if df.columns[0] != "Category":
    df.rename(columns={df.columns[0]: "Category"}, inplace=True)

###############################################################################
# STEP 4: PARSE THE HIERARCHY FROM 'Category'
#
# (Same logic as before: TOTAl => -1, no digits => 0, special patterns => level 2, etc.)
###############################################################################
def parse_category(cat_str):
    cat_str = cat_str.strip()
    if cat_str.upper() == "TOTAL":
        return -1, None, None
    if not re.search(r'\d', cat_str):
        return 0, None, None
    m_hyphen = re.match(r'^(\d{2})\s*-\s*(\d{2})', cat_str)
    if m_hyphen:
        start = int(m_hyphen.group(1))
        end = int(m_hyphen.group(2))
        special_range = {f"{num:02d}" for num in range(start, end + 1)}
        return 2, None, special_range
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

last_seen = {}
rows = []
children_map = {}

for i in range(len(df)):
    raw_cat = df.at[i, "Category"]
    cat_str = str(raw_cat)
    level, code, special_range = parse_category(cat_str)
    
    row_dict = {
        "index": i,
        "name": cat_str.strip(),
        "level": level,
        "code": code,
        "special_range": special_range,
        "parent": None,
        "values": {}
    }
    for col in df.columns:
        if col == "Category":
            continue
        row_dict["values"][col] = df.at[i, col]
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

value_cols = [c for c in df.columns if c != "Category"]

missing_initial = 0
total_fillable_cells = 0
for i in range(len(df)):
    for col in value_cols:
        val = df.at[i, col]
        if (isinstance(val, str) and val.strip().upper() == "C") or pd.isna(val):
            missing_initial += 1
        total_fillable_cells += 1
print(f"Initially, {missing_initial} missing cells out of {total_fillable_cells} total fillable cells.")

iteration_count = 0
max_iterations = 50
cells_filled_so_far = 0
log_entries = []

def get_row_name(idx):
    return rows[idx]["name"]

while True:
    iteration_count += 1
    changed_any = False
    row_map = {rd["index"]: rd for rd in rows}
    for rd in rows:
        i = rd["index"]
        parent_i = rd["parent"]
        child_idxs = children_map.get(i, [])
        for col in value_cols:
            val = rd["values"][col]
            if (isinstance(val, str) and val.strip().upper() == "C") or pd.isna(val):
                missing = True
            else:
                missing = False
            if not missing:
                continue

            # CASE A: sum of children
            if child_idxs:
                all_children_known = True
                child_sum = 0
                child_cells_used = []
                for cidx in child_idxs:
                    cval = row_map[cidx]["values"][col]
                    if (isinstance(cval, str) and cval.strip().upper() == "C") or pd.isna(cval):
                        all_children_known = False
                        break
                    child_sum += cval
                    child_cells_used.append(f"(row={cidx}, col={col}, val={cval})")
                if all_children_known:
                    rd["values"][col] = child_sum
                    changed_any = True
                    cells_filled_so_far += 1
                    log_entries.append({
                        "filled_row_index": i,
                        "filled_row_name": get_row_name(i),
                        "filled_col_name": col,
                        "method": "sum_of_children",
                        "cells_used": "; ".join(child_cells_used),
                        "computed_value": child_sum
                    })
                    continue

            # CASE B: parent minus siblings
            if parent_i is not None:
                parent_val = row_map[parent_i]["values"][col]
                if not ((isinstance(parent_val, str) and parent_val.strip().upper() == "C") or pd.isna(parent_val)):
                    sibling_idxs = [s for s in children_map[parent_i] if s != i]
                    siblings_known = True
                    sibling_sum = 0
                    sibling_cells_used = []
                    for sidx in sibling_idxs:
                        sval = row_map[sidx]["values"][col]
                        if (isinstance(sval, str) and sval.strip().upper() == "C") or pd.isna(sval):
                            siblings_known = False
                            break
                        sibling_sum += sval
                        sibling_cells_used.append(f"(row={sidx}, col={col}, val={sval})")
                    if siblings_known:
                        result_val = parent_val - sibling_sum
                        rd["values"][col] = result_val
                        changed_any = True
                        cells_filled_so_far += 1
                        log_entries.append({
                            "filled_row_index": i,
                            "filled_row_name": get_row_name(i),
                            "filled_col_name": col,
                            "method": "parent_minus_siblings",
                            "parent_cell_used": f"(row={parent_i}, col={col}, val={parent_val})",
                            "cells_used": "; ".join(child_cells_used),
                            "computed_value": result_val
                        })
                        continue
    if not changed_any or iteration_count >= max_iterations:
        break

###############################################################################
# INSERT A SMALL LOOP: Force any level 0 row that has parent=None
# to have the TOTAL row as parent, if a TOTAL row exists.
###############################################################################
total_idx = None
for rd in rows:
    if rd["level"] == -1:
        total_idx = rd["index"]
        break

if total_idx is not None:
    for rd in rows:
        if rd["level"] == 0 and rd["parent"] is None:
            rd["parent"] = total_idx
            children_map.setdefault(total_idx, []).append(rd["index"])

###############################################################################
# STEP 8: LEVEL-ORDER (TOP-DOWN) MEAN IMPUTATION FOR REMAINING MISSING CELLS
#
# same as your old approach that gave 4111 remain
###############################################################################
def impute_level(parent_level, child_level, col):
    groups = {}
    for r in rows:
        if r["level"] == child_level and r["parent"] is not None:
            p = r["parent"]
            if rows[p]["level"] == parent_level:
                groups.setdefault(p, []).append(r)
    for parent_idx, children in groups.items():
        parent_val = rows[parent_idx]["values"][col]
        if (isinstance(parent_val, str) and parent_val.strip().upper() == "C") or pd.isna(parent_val):
            continue
        known_sum = 0
        missing_children = []
        for child in children:
            val = child["values"][col]
            if (isinstance(val, str) and val.strip().upper() == "C") or pd.isna(val):
                missing_children.append(child)
            else:
                known_sum += val
        if missing_children:
            imputed_val = (parent_val - known_sum) / len(missing_children)
            for child in missing_children:
                child["values"][col] = imputed_val
                log_entries.append({
                    "filled_row_index": child["index"],
                    "filled_row_name": child["name"],
                    "filled_col_name": col,
                    "method": f"level{child['level']}_mean_imputation",
                    "parent_used": f"(row={parent_idx}, col={col}, val={parent_val})",
                    "imputed_value": imputed_val
                })

levels_order = [-1,0, 2, 3, 4]
for col in value_cols:
    for idx in range(len(levels_order)-1):
        parent_level = levels_order[idx]
        child_level = levels_order[idx+1]
        impute_level(parent_level, child_level, col)

###############################################################################
# STEP 9: WRITE THE FILLED VALUES BACK INTO THE ORIGINAL DataFrame
###############################################################################
for rd in rows:
    i = rd["index"]
    for col in value_cols:
        df.at[i, col] = rd["values"][col]

###############################################################################
# STEP 10: COUNT REMAINING MISSING CELLS
###############################################################################
missing_final = 0
for i in range(len(df)):
    for col in value_cols:
        val = df.at[i, col]
        if (isinstance(val, str) and val.strip().upper() == "C") or pd.isna(val):
            missing_final += 1

print(f"\nAfter {iteration_count} iteration(s) of hierarchy fill,")
print(f"Cells filled by hierarchy rules: {cells_filled_so_far}")
print(f"Remaining missing cells BEFORE top-down imputation: {missing_initial - cells_filled_so_far}")
print(f"Remaining missing cells AFTER top-down imputation: {missing_final}")

###############################################################################
# STEP 11: SAVE THE FINAL DATA AND THE LOG
###############################################################################
df.to_excel("FDI_data_filled.xlsx", index=True)
log_df = pd.DataFrame(log_entries)
log_df.to_excel("fdi_fill_log.xlsx", index=False)

print("\nDone. 'FDI_data_filled.xlsx' and 'fdi_fill_log.xlsx' have been written.")
