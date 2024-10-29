# Databricks notebook source
# INSTALAÇÃO DOS PACOTES NECESSÁRIOS
!pip install pdfplumber pandas pyPDF2

# COMMAND ----------

# IMPORTS NECESSÁRIOS
import pdfplumber
import pandas as pd
import os
import re
import PyPDF2

# COMMAND ----------

# MONTANDO O BLOB USANDO SAS TOKEN

blob_container = dbutils.secrets.get(scope="blob", key="container")
storage_account = dbutils.secrets.get(scope="blob", key="storage_account")
sas_token = dbutils.secrets.get(scope="blob", key="sas")

dbutils.fs.mount(
  source = f"wasbs://{blob_container}@{storage_account}.blob.core.windows.net",
  mount_point = "/mnt/blob",
  extra_configs = {f"fs.azure.sas.{blob_container}.{storage_account}.blob.core.windows.net": sas_token}
)

# COMMAND ----------

# Verificar se o contêiner está montado e listar os arquivos
display(dbutils.fs.ls("/mnt/blob/pdfs"))

# COMMAND ----------

csv_path = "/dbfs/mnt/blob/csvs"
pdf_files = [
    "/dbfs/mnt/blob/pdfs/preliminaries male.pdf",
    "/dbfs/mnt/blob/pdfs/preliminaries female.pdf",
    "/dbfs/mnt/blob/pdfs/1st round male.pdf",
    "/dbfs/mnt/blob/pdfs/1st round female.pdf",
    "/dbfs/mnt/blob/pdfs/semifinal male.pdf",
    "/dbfs/mnt/blob/pdfs/semifinal female.pdf",
    "/dbfs/mnt/blob/pdfs/final male.pdf",
    "/dbfs/mnt/blob/pdfs/final female.pdf"
]

# COMMAND ----------

#FUNÇÃO PARA EXTRAIR O CABECALHO
def extract_header_info(lines):
    event_type = None
    round_info = None
    date = None
    location = None

    event_pattern = re.compile(r"(Men's|Women's)\s100m", re.IGNORECASE)
    round_pattern = re.compile(r"(Round \d+|Semi-final|Final|Preliminary Round)", re.IGNORECASE)

    for line in lines:
        line = line.strip()

        event_match = event_pattern.search(line)
        if event_match:
            event_type = event_match.group()

        round_match = round_pattern.search(line)
        if round_match:
            round_info = round_match.group()

        date_match = re.search(r'\b[A-Z]{3}\s\d{1,2}\s[A-Z]{3}\s\d{4}\b', line)
        if date_match:
            date = date_match.group()

        if "Stade de France" in line:
            location = line.strip()

        if event_type and round_info and date and location:
            break

    return {
        "event_type": event_type,
        "round_info": round_info,
        "date": date,
        "location": location
}

# COMMAND ----------

#FUNÇÃO PARA PROCESSAR A INFORMAÇÃO DAS LINHAS
def process_line(line):
    line = re.sub(r'\s{2,}', ' ', line).strip()
    parts = line.split(' ')

    noc_codes = [
        "AFG", "ALB", "ALG", "AND", "ANG", "ANT", "ARG", "ARM", "ARU", "ASA", "AUS", "AUT", "AZE", "BAH", "BAN", "BAR", "BDI", "BEL", "BEN",
        "BER", "BHU", "BIH", "BIZ", "BLR", "BOL", "BOT", "BRA", "BRN", "BRU", "BUL", "BUR", "CAF", "CAM", "CAN", "CAY", "CGO", "CHA", "CHI",
        "CHN", "CIV", "CMR", "COD", "COK", "COL", "COM", "CPV", "CRC", "CRO", "CUB", "CYP", "CZE", "DEN", "DJI", "DMA", "DOM", "ECU", "EGY",
        "ERI", "ESA", "ESP", "EST", "ETH", "FIJ", "FIN", "FRA", "FSM", "GAB", "GAM", "GBR", "GBS", "GEQ", "GEO", "GER", "GHA", "GRE", "GRN",
        "GUA", "GUI", "GUM", "GUY", "HAI", "HKG", "HON", "HUN", "INA", "IND", "IRI", "IRL", "IRQ", "ISL", "ISR", "ISV", "ITA", "IVB", "JAM",
        "JOR", "JPN", "KAZ", "KEN", "KGZ", "KIR", "KOR", "KOS", "KSA", "KUW", "LAO", "LAT", "LBA", "LBN", "LBR", "LBY", "LCA", "LES", "LIE",
        "LTU", "LUX", "MAD", "MAL", "MAR", "MAS", "MAW", "MDA", "MDV", "MEX", "MGL", "MHL", "MKD", "MLI", "MLT", "MNE", "MON", "MOZ", "MRI",
        "MTN", "MYA", "NAM", "NCA", "NED", "NEP", "NGR", "NIG", "NOR", "NRU", "NZL", "OMA", "PAK", "PAN", "PAR", "PER", "PHI", "PLE", "PLW",
        "PNG", "POL", "POR", "PRK", "PUR", "QAT", "ROU", "RSA", "RUS", "RWA", "SAM", "SEN", "SEY", "SGP", "SKN", "SLE", "SLO", "SMR", "SOL",
        "SOM", "SRB", "SRI", "STP", "SUD", "SUI", "SUR", "SVK", "SWE", "SWZ", "SYR", "TAN", "TGA", "THA", "TJK", "TKM", "TLS", "TOG", "TPE",
        "TTO", "TUN", "TUR", "TUV", "UAE", "UGA", "UKR", "URU", "USA", "UZB", "VAN", "VEN", "VIE", "VIN", "YEM", "ZAM", "ZIM"
    ]

    special_codes = ['=SB', 'DNS', 'DQ', 'Fn', 'PB', 'SB', 'TR']

    index = None
    for i, item in enumerate(parts):
        if item in noc_codes:
            index = i
            break

    if index is None:
        print(f"No NOC code found in line: {line}")
        return None

    try:
        rank = parts[0] if len(parts) > 0 else None
        athlete_bib = parts[1] if len(parts) > 1 else None
        name = ' '.join(parts[2:index]) if len(parts) > 2 else None
        noc_code = parts[index] if len(parts) > index else None
        milliseconds_match = re.search(r'\((.*?)\)', ' '.join(parts[index + 4:]))

        if milliseconds_match:
            milliseconds = milliseconds_match.group(1)
            time = parts[index + 4] if len(parts) > index + 4 else None
            wind = parts[index + 6] if len(parts) > index + 6 else None
        else:
            milliseconds = None
            time = parts[index + 4] if len(parts) > index + 4 else None
            wind = parts[index + 5] if len(parts) > index + 5 else None

        heat = parts[index+1] if len(parts) > index + 1 else None
        lane = parts[index+2] if len(parts) > index + 2 else None
        pos = parts[index+3] if len(parts) > index + 3 else None
        result = 'Q' if 'Q' in parts[index + 6:] else 'q' if 'q' in parts[index + 6:] else None
        additional_info = next((code for code in parts[index + 6:] if code in special_codes), None)

        return [rank, athlete_bib, name, noc_code, heat, lane, pos, time, milliseconds, wind, result, additional_info]

    except IndexError as e:
        print(f"Error processing line: {line} - {e}")
        return None


# COMMAND ----------

#FUNÇÃO PARA PROCESSAR A INFORMAÇÃO DAS LINHAS DA FINAL MASCULINA
def process_line_final_men(line):
    line = re.sub(r'\s{2,}', ' ', line).strip()
    parts = line.split(' ')

    noc_codes = [
        "AFG", "ALB", "ALG", "AND", "ANG", "ANT", "ARG", "ARM", "ARU", "ASA", "AUS", "AUT", "AZE", "BAH", "BAN", "BAR", "BDI", "BEL", "BEN",
        "BER", "BHU", "BIH", "BIZ", "BLR", "BOL", "BOT", "BRA", "BRN", "BRU", "BUL", "BUR", "CAF", "CAM", "CAN", "CAY", "CGO", "CHA", "CHI",
        "CHN", "CIV", "CMR", "COD", "COK", "COL", "COM", "CPV", "CRC", "CRO", "CUB", "CYP", "CZE", "DEN", "DJI", "DMA", "DOM", "ECU", "EGY",
        "ERI", "ESA", "ESP", "EST", "ETH", "FIJ", "FIN", "FRA", "FSM", "GAB", "GAM", "GBR", "GBS", "GEQ", "GEO", "GER", "GHA", "GRE", "GRN",
        "GUA", "GUI", "GUM", "GUY", "HAI", "HKG", "HON", "HUN", "INA", "IND", "IRI", "IRL", "IRQ", "ISL", "ISR", "ISV", "ITA", "IVB", "JAM",
        "JOR", "JPN", "KAZ", "KEN", "KGZ", "KIR", "KOR", "KOS", "KSA", "KUW", "LAO", "LAT", "LBA", "LBN", "LBR", "LBY", "LCA", "LES", "LIE",
        "LTU", "LUX", "MAD", "MAL", "MAR", "MAS", "MAW", "MDA", "MDV", "MEX", "MGL", "MHL", "MKD", "MLI", "MLT", "MNE", "MON", "MOZ", "MRI",
        "MTN", "MYA", "NAM", "NCA", "NED", "NEP", "NGR", "NIG", "NOR", "NRU", "NZL", "OMA", "PAK", "PAN", "PAR", "PER", "PHI", "PLE", "PLW",
        "PNG", "POL", "POR", "PRK", "PUR", "QAT", "ROU", "RSA", "RUS", "RWA", "SAM", "SEN", "SEY", "SGP", "SKN", "SLE", "SLO", "SMR", "SOL",
        "SOM", "SRB", "SRI", "STP", "SUD", "SUI", "SUR", "SVK", "SWE", "SWZ", "SYR", "TAN", "TGA", "THA", "TJK", "TKM", "TLS", "TOG", "TPE",
        "TTO", "TUN", "TUR", "TUV", "UAE", "UGA", "UKR", "URU", "USA", "UZB", "VAN", "VEN", "VIE", "VIN", "YEM", "ZAM", "ZIM"
    ]

    special_codes = ['=SB', 'DNS', 'DQ', 'Fn', 'PB', 'SB', 'TR']

    index = None
    for i, item in enumerate(parts):
        if item in noc_codes:
            index = i
            break

    if index is None:
        print(f"No NOC code found in line: {line}")
        return None

    try:
        rank = parts[0] if len(parts) > 0 else None
        athlete_bib = parts[1] if len(parts) > 1 else None
        name = ' '.join(parts[2:index]) if len(parts) > 2 else None
        noc_code = parts[index] if len(parts) > index else None
        milliseconds_match = re.search(r'\((.*?)\)', ' '.join(parts[index + 3:]))

        if milliseconds_match:
            milliseconds = milliseconds_match.group(1)
            time = parts[index + 2] if len(parts) > index + 2 else None
        else:
            milliseconds = None
            time = parts[index + 2] if len(parts) > index + 2 else None

        wind = "+1.0"
        heat = None
        lane = parts[index+1] if len(parts) > index + 1 else None
        pos = None
        result = 'Q' if rank == "1" else None
        additional_info = next((code for code in parts[index + 3:] if code in special_codes), None)

        return [rank, athlete_bib, name, noc_code, heat, lane, pos, time, milliseconds, wind, result, additional_info]

    except IndexError as e:
        print(f"Error processing line: {line} - {e}")
        return None

# COMMAND ----------

#FUNÇÃO PARA PROCESSAR A INFORMAÇÃO DAS LINHAS DO ARQUIVO FEMININO
def process_line_final_female(line):
    line = re.sub(r'\s{2,}', ' ', line).strip()
    parts = line.split(' ')

    noc_codes = [
        "AFG", "ALB", "ALG", "AND", "ANG", "ANT", "ARG", "ARM", "ARU", "ASA", "AUS", "AUT", "AZE", "BAH", "BAN", "BAR", "BDI", "BEL", "BEN",
        "BER", "BHU", "BIH", "BIZ", "BLR", "BOL", "BOT", "BRA", "BRN", "BRU", "BUL", "BUR", "CAF", "CAM", "CAN", "CAY", "CGO", "CHA", "CHI",
        "CHN", "CIV", "CMR", "COD", "COK", "COL", "COM", "CPV", "CRC", "CRO", "CUB", "CYP", "CZE", "DEN", "DJI", "DMA", "DOM", "ECU", "EGY",
        "ERI", "ESA", "ESP", "EST", "ETH", "FIJ", "FIN", "FRA", "FSM", "GAB", "GAM", "GBR", "GBS", "GEQ", "GEO", "GER", "GHA", "GRE", "GRN",
        "GUA", "GUI", "GUM", "GUY", "HAI", "HKG", "HON", "HUN", "INA", "IND", "IRI", "IRL", "IRQ", "ISL", "ISR", "ISV", "ITA", "IVB", "JAM",
        "JOR", "JPN", "KAZ", "KEN", "KGZ", "KIR", "KOR", "KOS", "KSA", "KUW", "LAO", "LAT", "LBA", "LBN", "LBR", "LBY", "LCA", "LES", "LIE",
        "LTU", "LUX", "MAD", "MAL", "MAR", "MAS", "MAW", "MDA", "MDV", "MEX", "MGL", "MHL", "MKD", "MLI", "MLT", "MNE", "MON", "MOZ", "MRI",
        "MTN", "MYA", "NAM", "NCA", "NED", "NEP", "NGR", "NIG", "NOR", "NRU", "NZL", "OMA", "PAK", "PAN", "PAR", "PER", "PHI", "PLE", "PLW",
        "PNG", "POL", "POR", "PRK", "PUR", "QAT", "ROU", "RSA", "RUS", "RWA", "SAM", "SEN", "SEY", "SGP", "SKN", "SLE", "SLO", "SMR", "SOL",
        "SOM", "SRB", "SRI", "STP", "SUD", "SUI", "SUR", "SVK", "SWE", "SWZ", "SYR", "TAN", "TGA", "THA", "TJK", "TKM", "TLS", "TOG", "TPE",
        "TTO", "TUN", "TUR", "TUV", "UAE", "UGA", "UKR", "URU", "USA", "UZB", "VAN", "VEN", "VIE", "VIN", "YEM", "ZAM", "ZIM"
    ]

    special_codes = ['=SB', 'DNS', 'DQ', 'Fn', 'PB', 'SB', 'TR']

    index = None
    for i, item in enumerate(parts):
        if item in noc_codes:
            index = i
            break

    if index is None:
        print(f"No NOC code found in line: {line}")
        return None

    try:
        rank = parts[0] if len(parts) > 0 else None
        athlete_bib = parts[1] if len(parts) > 1 else None
        name = ' '.join(parts[2:index]) if len(parts) > 2 else None
        noc_code = parts[index] if len(parts) > index else None
        milliseconds = None
        time = parts[index + 8] if len(parts) > index + 8 else None
        wind = parts[index + 7] if len(parts) > index + 7 else None
        heat = None
        lane = parts[index+5] if len(parts) > index + 5 else None
        pos = parts[index+6] if len(parts) > index + 6 else None
        result = 'Q' if rank == "1" else None
        additional_info = "NR" if rank == "1" else None

        return [rank, athlete_bib, name, noc_code, heat, lane, pos, time, milliseconds, wind, result, additional_info]

    except IndexError as e:
        print(f"Error processing line: {line} - {e}")
        return None

# COMMAND ----------

all_data = []

for pdf_file in pdf_files:
    data = []
    header_info = {}

    with pdfplumber.open(pdf_file) as pdf:
        for page in pdf.pages:
            text = page.extract_text()
            lines = text.split('\n')

            if not header_info:
                header_info = extract_header_info(lines)

            for line in lines:
                if re.match(r'^\d', line):
                  if pdf_file == '/dbfs/mnt/blob/pdfs/final male.pdf':
                    result = process_line_final_men(line)
                    if result:
                        data.append(result)
                  elif pdf_file == '/dbfs/mnt/blob/pdfs/final female.pdf':
                    result = process_line_final_female(line)
                    if result:
                        data.append(result)
                  else:
                    result = process_line(line)
                    if result:
                        data.append(result)

    df = pd.DataFrame(data, columns=["Rank", "Athlete Bib", "Name", "NOC Code","Heat", "Lane", "Pos","Time", "Milliseconds", "Wind","Result", "Additional Info"])

    df.insert(0, 'Event Type', header_info.get('event_type', 'Unknown Event'))
    df.insert(1, 'Round', header_info.get('round_info', 'Preliminary Round'))
    df.insert(2, 'Date', header_info.get('date', 'Unknown Date'))
    df.insert(3, 'Location', header_info.get('location', 'Unknown Location'))

    all_data.append(df)

final_df = pd.concat(all_data, ignore_index=True)

output_csv = os.path.join(csv_path, 'athletics 100m.csv')
final_df.to_csv(output_csv, index=False)

print(f"Todos os dados foram salvos no arquivo {output_csv}")
