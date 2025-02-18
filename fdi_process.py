import pandas as pd

###############################################################################
# STEP 1) READ THE EXCEL FILE & RESET THE INDEX
#    - We assume the file has two header rows.
#    - We reset_index(drop=True) so df.index = 0..N.
###############################################################################
df = pd.read_excel(
    "FDI.xlsx",     # <-- Change to your actual file path
    sheet_name="Por sector", # <-- Adjust if needed
    header=[0, 1]        # first two rows used as headers
)
df.reset_index(drop=True, inplace=True)  # Ensures row indices are 0..N

###############################################################################
# STEP 2) FLATTEN THE (TWO-ROW) HEADERS
###############################################################################
temp_cols = []
for col_pair in df.columns:
    # col_pair might be ('1999','1'), ('Unnamed: 0_level_0','Unnamed: 0_level_1'), etc.
    col_str = "_".join(str(x) for x in col_pair if x)
    temp_cols.append(col_str)
df.columns = temp_cols

###############################################################################
# STEP 3) CLEAN UP "Unnamed:" & rename the first column to "Category" if needed
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
# STEP 4) PARSE THE HIERARCHY FROM 'Category' USING NUMBER OF LEADING DIGITS
#    - If cat_str.upper() == "TOTAL", set level = -1
#    - Otherwise, parse the leading digits from the start => level = # of digits
###############################################################################
rows = []
children_map = {}
stack = []

def get_level_from_digits(cat_str):
    """
    Return the count of leading digits at the start of cat_str.
    Examples:
      "31-33 Industrias" -> 2
      "43 y 46 Comercio" -> 2
      "111 Another" -> 3
      "Agricultura" -> 0 (no leading digits)
    """
    cat_str = cat_str.lstrip()  # remove any leading spaces
    digits = ""
    for ch in cat_str:
        if ch.isdigit():
            digits += ch
        else:
            break
    return len(digits)

for i in range(len(df)):
    raw_cat = df.at[i, "Category"]
    cat_str = str(raw_cat).strip()

    if cat_str.upper() == "TOTAL":
        level = -1
    else:
        level = get_level_from_digits(cat_str)

    row_dict = {
        "index": i,
        "name": cat_str,
        "level": level,
        "parent": None,
        "values": {}
    }

    # Copy numeric/time columns into row_dict["values"]
    for col in df.columns:
        if col == "Category":
            continue
        row_dict["values"][col] = df.at[i, col]

    rows.append(row_dict)

    # Stack-based parent/child determination
    while stack and stack[-1][0] >= level:
        stack.pop()

    if stack:
        parent_idx = stack[-1][1]
        row_dict["parent"] = parent_idx
        children_map.setdefault(parent_idx, []).append(i)

    stack.append((level, i))

###############################################################################
# STEP 5) SELECT COLUMNS TO FILL (ALL EXCEPT "Category")
###############################################################################
value_cols = [c for c in df.columns if c != "Category"]

###############################################################################
# STEP 6) COUNT MISSING AT THE START
###############################################################################
missing_initial = 0
total_fillable_cells = 0
for i in range(len(df)):
    for col in value_cols:
        val = df.at[i, col]
        if (isinstance(val, str) and val.strip().upper() == "C") or pd.isna(val):
            missing_initial += 1
        total_fillable_cells += 1

print(f"Initially, {missing_initial} missing cells out of {total_fillable_cells} total fillable cells.")

###############################################################################
# STEP 7) ITERATIVE FILL WITH LOG
###############################################################################
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
            # Missing?
            if (isinstance(val, str) and val.strip().upper() == "C") or pd.isna(val):
                missing = True
            else:
                missing = False

            if not missing:
                continue

            # A) sum of children
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

            # B) parent minus siblings
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
                            "cells_used": "; ".join(sibling_cells_used),
                            "computed_value": result_val
                        })
                        continue

    if not changed_any or iteration_count >= max_iterations:
        break

###############################################################################
# STEP 8) WRITE THE FILLED VALUES BACK INTO df
###############################################################################
for rd in rows:
    i = rd["index"]
    for col in value_cols:
        df.at[i, col] = rd["values"][col]

###############################################################################
# STEP 9) COUNT HOW MANY CELLS REMAIN MISSING
###############################################################################
missing_final = 0
for i in range(len(df)):
    for col in value_cols:
        val = df.at[i, col]
        if (isinstance(val, str) and val.strip().upper() == "C") or pd.isna(val):
            missing_final += 1

print(f"\nFilling completed after {iteration_count} iteration(s).")
print(f"Cells filled: {cells_filled_so_far}")
print(f"Remaining missing: {missing_final}")

###############################################################################
# STEP 10) SAVE THE FINAL DATA AND THE LOG
###############################################################################
df.to_excel("FDI_data_filled.xlsx", index=True)  # index=True or False as you prefer

log_df = pd.DataFrame(log_entries)
log_df.to_excel("fdi_fill_log.xlsx", index=False)

print("\nDone. 'FDI_data_filled.xlsx' and 'fdi_fill_log.xlsx' have been written.")
