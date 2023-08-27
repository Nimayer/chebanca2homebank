#!/usr/bin/env python3

# chebanca2homebank.py: Convert CheBanca xslx to HomeBank CSV
# Copyright (C) 2023  Federico Amedeo Izzo

# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.

# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.

# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.

import sys
import polars as pl
import re

if len(sys.argv) < 2:
    print(f"Usage: {sys.argv[0]} chebanca.xlsx")
    print("Convert from CheBanca xlsx report to HomeBank CSV\n")
    exit(-1)

input_file = sys.argv[1]
output_file = input_file[:-5] + ".csv"

# "Tipologia contains type and payee separated by " - " or " *"
chebanca_type_regex = r"(?P<type>\S+\s*\S*)(?:\s-\s|\s\*)(?P<info>.+)"
chebanca_payee_regex = r"\*(?P<payee_paypal>[A-Za-z]+(?:\s[A-Za-z]+)*)\d+|"\
                       r"\sA\s(?:I|E\s)?\(\w{3}\)\s(?P<payee_pos>\w+(?:\s?\s?\.?\w+)*)|"\
                       r"RIF:\d+(?:ORD|BEN)\.\s(?P<payee_bonifico>[A-Za-z]+(?:\s?\s?\.?[A-Za-z]*){1})|"\
                       r"SDD\s-\s(?P<payee_addebito>[A-Za-z]+(?:\s[A-Za-z]+)*).*"

homebank_types = {"None": 0,
                  "Credit card": 1,
                  "Check": 2,
                  "Cash": 3,
                  "Bank Transfer": 4,
                  # "Internal Transfer": 5 Do not use, reserved for HomeBank
                  "Debit Card": 6,
                  "Standing order": 7,
                  "Electronic payment": 8,
                  "Deposit": 9,
                  "FI Fee": 10,
                  "Direct Debit": 11
                  # Unclear if supported by HomeBank
                  }
chebanca_types = {"Stipendio": homebank_types["Bank Transfer"], 
                  "Bonif. v/fav.": homebank_types["Bank Transfer"], 
                  "Disposizione": homebank_types["Bank Transfer"], 
                  "Addebito canone": homebank_types["FI Fee"], 
                  "Pagam. POS": homebank_types["Debit Card"], 
                  "Addebito SDD": homebank_types["Direct Debit"], 
                  "POS-PAYPAL": homebank_types["Electronic payment"], 
                  "cont. ATM": homebank_types["None"], 
                  "Bancomat": homebank_types["None"]}

# Combine non-empty regex groups into a string
def search_payee(s):
    match = re.findall(chebanca_payee_regex, s)
    if match: return "".join(match[0])

# Read Excel file to Polars DataFrame
df_cb = pl.read_excel(input_file, read_csv_options={"skip_rows": 14, "ignore_errors": True})
# Drop last lines containing totals and empty first column
df_cb = df_cb.head(df_cb.height - 2)
df_cb = df_cb.drop("")
print(f"CheBanca table \n{df_cb}")
types = df_cb.select("Tipologia")["Tipologia"].to_list()    

# Convert from CheBanca to HomeBank format
# * "Data valuta" is the date the payment was requested
df_hb = df_cb.select(
    [
        pl.col("Data valuta").alias("date"),
        pl.col("Tipologia").str.extract(chebanca_type_regex, 1)
            .map_dict(chebanca_types).alias("payment"),
        pl.col("Tipologia").str.extract(chebanca_type_regex, 2).alias("info"),
        pl.col("Tipologia").apply(search_payee).apply(lambda s: s.capitalize()).alias("payee"),
        pl.col("Tipologia").str.extract(chebanca_type_regex, 1).alias("memo"),
        pl.col("Entrate").fill_null(pl.col("Uscite")).alias("amount"),
        pl.lit(None).alias("category"),
        pl.lit(None).alias("tags")
    ]
)
details_set = set(df_hb.select("memo")["memo"].to_list())
print(f"Payment types found: {details_set}")
print(f"HomeBank table \n{df_hb}")
# HomeBank uses ; as separator
df_hb.write_csv(output_file, separator=";")
print(f"HomeBank table written to {output_file}\n")
