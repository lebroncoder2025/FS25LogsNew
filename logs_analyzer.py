import os
import re
import sys
import ftplib
import traceback
import json
import ast
from datetime import datetime
from collections import Counter
import pandas as pd
import logging
import traceback

# Awaryjny wpis do debug.txt
with open("debug.txt", "a", encoding="utf-8") as f:
    f.write("✅ Skrypt uruchomiony\n")

# Foldery
os.makedirs("logs", exist_ok=True)
LOG_DIR = "log_cache"
REPORT_DIR = "docs"
os.makedirs(LOG_DIR, exist_ok=True)
os.makedirs(REPORT_DIR, exist_ok=True)

# Pliki do logowania
ERROR_LOG = os.path.join("logs", "error_log.txt")
UNPARSED_LOG = os.path.join("logs", "unparsed_lines.txt")

# Konfiguracja logging
logging.basicConfig(
    filename="logs/software_logs.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    encoding="utf-8"
)

# Przekierowanie stdout/stderr do logging
class LoggerWriter:
    def __init__(self, logger, level):
        self.logger = logger
        self.level = level

    def write(self, message):
        if message.rstrip():
            self.logger.log(self.level, message.rstrip())

    def flush(self):
        for handler in self.logger.handlers:
            handler.flush()

sys.stdout = LoggerWriter(logging.getLogger(), logging.INFO)
sys.stderr = LoggerWriter(logging.getLogger(), logging.ERROR)

# Zmienne środowiskowe
FTP_HOST = os.environ.get("FTP_HOST")
FTP_PORT = int(os.environ.get("FTP_PORT", "21"))
FTP_USER = os.environ.get("FTP_USER")
FTP_PASS = os.environ.get("FTP_PASS")
FTP_DIR = os.environ.get("FTP_DIR")
FTP_DIR2 = os.environ.get("FTP_DIR2")

print("✅ Konfiguracja zakończona — startuję analizę...")

# Wzorce do parsowania
TIMESTAMP = re.compile(r"(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3})")
EVENTS = {
    "player_connected": r"(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3})\s+([^\s].*?)\s+(joined the game)",
    "player_disconnected": r"(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3})\s+([^\s].*?)\s+(lost connection to the game|left the game)",
    "executed_command": r"(?:Executed command|Admin command|Command): (\w+)\s*(.*)",
    "admin_action": r"ADMIN: (.*)",
    "lua_error": r"Error: Running LUA method '(\w+)'. (.*)",
    "warning_stream": r"Warning: StreamWriteTimestamp (.*)",
    "memory_warning": r"Lua memory usage has reached (\d+) KB; (.*)",
    "file_load": r"(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3}) (.+) \(([\d.]+) ms\)",
    "network_unknown_target": r"Warning: Send called with unknown target address",
    "network_decrypt_error": r"Warning: Could not decrypt received packet",
    "dlc_load": r"Available dlc: \(Hash: ([a-f0-9]+)\) \(Version: ([\d\.]+)\) (.+)",
    "mod_load": r"Available mod: \(Hash: ([a-f0-9]+)\) \(Version: ([\d\.]+)\) (.+)",
    "save_game": r"Game saved successfully|Saving savegame|Saved game",
    "duplicate_l10n": r"Warning: Duplicate l10n entry '(.+)' in mod '(.+)'",
    "mod_warning": r"Warning: (.+) in mod '(.+)'",
    "real_dirt_color": r"Real Dirt Color successfully applied to (.+)",
    "error": r"Error: (.+)",
    "warning": r"Warning: (.+)",
    "system_info": r"(GIANTS Engine Runtime|Copyright|Application|PID|Main System|CPU|Virtual Cores|Memory|OS|Physics System|Version|Thread|Sound System|Driver|Render System|NullConsoleDevice|Started \d+ threads|Hardware Profile|Level|Recommended Window Size|UI Scaling Factor|3D Scaling Factor|View Distance Factor|LOD Distance Factor)",
    "direct_storage": r"\[DirectStorage\] (.*)",
    "value_line": r"(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3}) (\d+\.\d+)",
    "info_add": r"(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3})   Info: (.*)",
    "forestry_helper": r"(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3}) FS25_ForestryHelper: (.*)",
    "density_map": r"(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3}) FTG '(.*)' max needed CPU instances = ([\d.]+) MB / ([\d.]+) MB",
    "master_login": r"Info: \[Easy Development Controls\] User (\w+) has logged in as master user.",
}

# Funkcja do konwersji Details na słownik
def parse_details(details):
    if isinstance(details, dict):
        return details
    elif isinstance(details, str):
        try:
            return ast.literal_eval(details)
        except (ValueError, SyntaxError):
            logging.warning(f"⚠️ Nie można sparsować Details jako słownik: {details}")
            return {}
    return {}

# Pobieranie logów z FTP
def download_logs(DIR):
    try:
        logging.info("🔄 Łączenie z FTP...")
        with ftplib.FTP() as ftp:
            ftp.connect(FTP_HOST, FTP_PORT)
            ftp.login(FTP_USER, FTP_PASS)
            ftp.cwd(DIR)
            entries = []
            ftp.retrlines("MLSD", entries.append)
            files = [line.split(";")[-1].strip() for line in entries if line.endswith(".txt")]
            logging.info(f"📄 Znaleziono {len(files)} plików logów.")
            
            for filename in files:
                local_path = os.path.join(LOG_DIR, filename)
                download = True
                if os.path.exists(local_path):
                    remote_size = ftp.size(filename) if hasattr(ftp, 'size') else None
                    local_size = os.path.getsize(local_path)
                    if remote_size is not None and remote_size == local_size:
                        try:
                            remote_mtime = ftp.sendcmd("MDTM " + filename)[4:].strip()
                            remote_time = datetime.strptime(remote_mtime, "%Y%m%d%H%M%S")
                            local_time = datetime.fromtimestamp(os.path.getmtime(local_path))
                            if remote_time <= local_time:
                                logging.info(f"⏭️ Pominięto (aktualny): {filename}")
                                download = False
                        except:
                            if remote_size == local_size:
                                logging.info(f"⏭️ Pominięto (ten sam rozmiar): {filename}")
                                download = False
                            else:
                                logging.info(f"🔄 Pobieram (inny rozmiar): {filename}")
                else:
                    logging.info(f"🔄 Pobieram (nowy plik): {filename}")
                
                if download:
                    with open(local_path, "wb") as f:
                        ftp.retrbinary(f"RETR " + filename, f.write)
                    logging.info(f"✅ Pobrano: {filename}")
    except Exception as e:
        with open(ERROR_LOG, "a", encoding="utf-8") as f:
            f.write(f"{datetime.now()}: Błąd FTP: {e}\n{traceback.format_exc()}\n")
        logging.error(f"❌ Błąd FTP: {e}")

# Parsowanie linii
def parse_line(line):
    try:
        line = line.encode('utf-8', errors='ignore').decode('utf-8').strip()
        if not line:
            return None

        entry = {
            "Timestamp": None,
            "EventType": "unknown",
            "RawLine": line,
            "LineType": "UNKNOWN",
            "Details": {}
        }

        ts_match = TIMESTAMP.search(line)
        if ts_match:
            try:
                entry["Timestamp"] = datetime.strptime(ts_match.group(1), "%Y-%m-%d %H:%M:%S.%f")
            except ValueError as e:
                logging.warning(f"⚠️ Nieprawidłowy format timestamp w linii: {line} - {e}")

        if "INFO:" in line:
            entry["LineType"] = "INFO"
        elif "ADMIN:" in line:
            entry["LineType"] = "ADMIN"
        elif "ERROR:" in line or "Error" in line:
            entry["LineType"] = "ERROR"
        elif "WARNING:" in line or "Warning" in line:
            entry["LineType"] = "WARNING"

        matched = False
        for etype, pattern in EVENTS.items():
            try:
                match = re.search(pattern, line, re.IGNORECASE)
                if match:
                    entry["EventType"] = etype
                    matched = True
                    try:
                        if etype in ["player_connected", "player_disconnected"]:
                            player_name = match.group(2).strip() if match.group(2) else None
                            if not player_name:
                                entry["Details"]["Error"] = "Brak nazwy gracza"
                            else:
                                entry["Details"]["PlayerName"] = player_name
                                entry["Timestamp"] = datetime.strptime(match.group(1), "%Y-%m-%d %H:%M:%S.%f")
                        elif etype == "file_load":
                            entry["Details"]["Path"] = match.group(2).strip()
                            entry["Details"]["LoadTimeMS"] = float(match.group(3))
                            entry["Timestamp"] = datetime.strptime(match.group(1), "%Y-%m-%d %H:%M:%S.%f")
                        elif etype == "real_dirt_color":
                            entry["Details"]["AppliedTo"] = match.group(1).strip()
                        elif etype == "executed_command":
                            entry["Details"]["Command"] = match.group(1)
                            entry["Details"]["Args"] = match.group(2).strip()
                        elif etype == "admin_action":
                            entry["Details"]["Message"] = match.group(1).strip()
                        elif etype == "lua_error":
                            entry["Details"]["Method"] = match.group(1)
                            entry["Details"]["Message"] = match.group(2)
                        elif etype == "memory_warning":
                            entry["Details"]["MemoryKB"] = int(match.group(1))
                            entry["Details"]["Message"] = match.group(2)
                        elif etype in ["dlc_load", "mod_load"]:
                            entry["Details"]["Hash"] = match.group(1)
                            entry["Details"]["Version"] = match.group(2)
                            entry["Details"]["Name"] = match.group(3).strip()
                        elif etype in ["duplicate_l10n", "mod_warning"]:
                            entry["Details"]["Entry"] = match.group(1)
                            entry["Details"]["Mod"] = match.group(2)
                        elif etype in ["error", "warning"]:
                            entry["Details"]["Message"] = match.group(1)
                        elif etype == "system_info":
                            entry["Details"]["Info"] = match.group(1)
                        elif etype == "direct_storage":
                            entry["Details"]["Message"] = match.group(1)
                        elif etype == "value_line":
                            entry["Timestamp"] = datetime.strptime(match.group(1), "%Y-%m-%d %H:%M:%S.%f")
                            entry["Details"]["Value"] = float(match.group(2))
                        elif etype == "info_add":
                            entry["Timestamp"] = datetime.strptime(match.group(1), "%Y-%m-%d %H:%M:%S.%f")
                            entry["Details"]["Message"] = match.group(2)
                        elif etype == "forestry_helper":
                            entry["Timestamp"] = datetime.strptime(match.group(1), "%Y-%m-%d %H:%M:%S.%f")
                            entry["Details"]["Message"] = match.group(2)
                        elif etype == "density_map":
                            entry["Timestamp"] = datetime.strptime(match.group(1), "%Y-%m-%d %H:%M:%S.%f")
                            entry["Details"]["Path"] = match.group(2)
                            entry["Details"]["MaxCPU"] = float(match.group(3))
                            entry["Details"]["TotalMB"] = float(match.group(4))
                        elif etype == "master_login":
                            entry["Details"]["User"] = match.group(1)
                    except Exception as e:
                        entry["Details"]["Error"] = f"Błąd parsowania szczegółów dla {etype}: {e}"
                        logging.error(f"❌ Błąd parsowania szczegółów dla {etype} w linii: {line} - {e}")
                    break
            except re.error as e:
                logging.error(f"❌ Błąd w wyrażeniu regularnym dla {etype}: {e} - Wzorzec: {pattern}")
                continue

        if not matched:
            entry["EventType"] = "other"
            entry["Details"]["Message"] = line
            if ts_match:
                try:
                    entry["Timestamp"] = datetime.strptime(ts_match.group(1), "%Y-%m-%d %H:%M:%S.%f")
                except ValueError as e:
                    logging.warning(f"⚠️ Nieprawidłowy timestamp w niepasującej linii: {line} - {e}")

        return entry
    except Exception as e:
        with open(ERROR_LOG, "a", encoding="utf-8") as f:
            f.write(f"{datetime.now()}: Błąd parsowania linii: {line} - {e}\n{traceback.format_exc()}\n")
        logging.error(f"❌ Błąd parsowania linii: {line} - {e}")
        return None

# Analiza wszystkich logów w katalogu
def analyze_logs():
    try:
        events = []
        total_lines = 0
        unparsed_lines = 0
        event_counts = Counter()
        for fname in os.listdir(LOG_DIR):
            if fname.endswith(".txt"):
                logging.info(f"🔍 Analizuję: {fname}")
                file_events = 0
                with open(os.path.join(LOG_DIR, fname), "r", encoding="utf-8", errors="replace") as f:
                    lines = f.readlines()
                    total_lines += len(lines)
                    for line in lines:
                        parsed = parse_line(line)
                        if parsed:
                            events.append(parsed)
                            event_counts[parsed["EventType"]] += 1
                            file_events += 1
                        else:
                            unparsed_lines += 1
                logging.info(f"📄 Plik {fname}: {file_events} zdarzeń")
        logging.info(f"📊 Zebrano {len(events)} zdarzeń z {total_lines} linii. Nieparsowanych linii: {unparsed_lines}.")
        logging.info("📈 Rozkład typów zdarzeń:")
        for etype, count in event_counts.items():
            logging.info(f"  - {etype}: {count}")
        return events, event_counts
    except Exception as e:
        with open(ERROR_LOG, "a", encoding="utf-8") as f:
            f.write(f"{datetime.now()}: Błąd analizy logów: {e}\n{traceback.format_exc()}\n")
        logging.error(f"❌ Błąd analizy logów: {e}")
        return [], Counter()

# Statystyki błędów, ostrzeżeń i admina
def detect_errors_and_stats(events):
    try:
        df = pd.DataFrame(events)
        errors = df[df["LineType"] == "ERROR"]
        warnings = df[df["LineType"] == "WARNING"]
        
        logging.info(f"❗ Wykryto {len(errors)} błędów i {len(warnings)} ostrzeżeń.")
        
        warning_types = Counter(warnings["EventType"])
        logging.info("📈 Statystyki ostrzeżeń:")
        for typ, count in warning_types.items():
            logging.info(f"  - {typ}: {count}")
        
        mod_issues = Counter(w["Details"].get("Mod", "Unknown") for _, w in warnings.iterrows() if isinstance(w["Details"], dict) and "Mod" in w["Details"])
        if mod_issues:
            logging.info("🛠️ Mody z problemami:")
            for mod, count in mod_issues.items():
                logging.info(f"  - {mod}: {count} issues")
        
        mods = df[df["EventType"] == "mod_load"]
        dlcs = df[df["EventType"] == "dlc_load"]
        logging.info(f"📦 Załadowano {len(mods)} modów i {len(dlcs)} DLC.")

        sessions_df, admin_cmds = admin_player_stats(events)
        
        return errors, warnings, warning_types, mod_issues, sessions_df, admin_cmds
    except Exception as e:
        with open(ERROR_LOG, "a", encoding="utf-8") as f:
            f.write(f"{datetime.now()}: Błąd w detect_errors_and_stats: {e}\n{traceback.format_exc()}\n")
        logging.error(f"❌ Błąd w detect_errors_and_stats: {e}")
        return pd.DataFrame(), pd.DataFrame(), Counter(), Counter(), pd.DataFrame(), pd.DataFrame()

# Statystyki admina i graczy
def admin_player_stats(events):
    try:
        df = pd.DataFrame(events)
        df["Timestamp"] = pd.to_datetime(df["Timestamp"], errors="coerce")

        connects = df[df["EventType"] == "player_connected"].sort_values("Timestamp")
        disconnects = df[df["EventType"] == "player_disconnected"].sort_values("Timestamp")
        sessions = []
        active_connections = {}

        all_events = pd.concat([connects, disconnects]).sort_values("Timestamp")
        for _, event in all_events.iterrows():
            player = event["Details"].get("PlayerName", None) if isinstance(event["Details"], dict) else parse_details(event["Details"]).get("PlayerName", None)
            if not player:
                continue

            if event["EventType"] == "player_connected":
                if player not in active_connections:
                    active_connections[player] = event["Timestamp"]
                else:
                    logging.warning(f"⚠️ Gracz {player} już połączony w czasie {event['Timestamp']}, ignoruję powtórne połączenie.")
            elif event["EventType"] == "player_disconnected":
                if player in active_connections:
                    start_time = active_connections.pop(player)
                    duration = (event["Timestamp"] - start_time).total_seconds() / 60
                    if duration > 1440:
                        logging.warning(f"⚠️ Sesja gracza {player} przekroczyła 24h ({duration:.2f} min), ograniczam do 1440 min.")
                        duration = 1440
                    sessions.append({
                        "Player": player,
                        "Start": start_time,
                        "End": event["Timestamp"],
                        "Duration": duration
                    })

        for player, start_time in active_connections.items():
            logging.warning(f"⚠️ Gracz {player} nie ma disconnect, połączenie od {start_time}.")
            sessions.append({
                "Player": player,
                "Start": start_time,
                "End": None,
                "Duration": 0
            })

        sessions_df = pd.DataFrame(sessions)
        if not sessions_df.empty:
            logging.info(f"👥 Sesje graczy (min): \n{sessions_df.to_string()}")
        else:
            logging.info("⚠️ Brak sesji graczy.")

        admin_cmds = df[df["EventType"].isin(["executed_command", "admin_action", "master_login"])]
        if not admin_cmds.empty:
            cmd_counts = admin_cmds["Details"].apply(lambda x: parse_details(x).get("Command", parse_details(x).get("Message", parse_details(x).get("User", "Unknown")))).value_counts()
            logging.info(f"🛡️ Komendy admina: \n{cmd_counts}")
        else:
            logging.info("⚠️ Brak komend admina.")

        if not disconnects.empty:
            disc_counts = disconnects["Details"].apply(lambda x: parse_details(x).get("PlayerName", "Unknown")).value_counts()
            if disc_counts.max() > 3:
                logging.info(f"🚨 Problematyczni gracze (wiele disconnectów): \n{disc_counts[disc_counts > 3]}")

        return sessions_df, admin_cmds
    except Exception as e:
        with open(ERROR_LOG, "a", encoding="utf-8") as f:
            f.write(f"{datetime.now()}: Błąd w admin_player_stats: {e}\n{traceback.format_exc()}\n")
        logging.error(f"❌ Błąd w admin_player_stats: {e}")
        return pd.DataFrame(), pd.DataFrame()

# Zapisane gry i dane do wykresów
def handle_saves(events):
    try:
        df = pd.DataFrame(events)
        df_saves = df[df["EventType"] == "save_game"].sort_values("Timestamp")
        charts = {}
        if not df_saves.empty:
            logging.info(f"💾 Znaleziono {len(df_saves)} zapisów gry.")
            
            df_saves["Count"] = 1
            df_saves_per_hour = df_saves.groupby(df_saves["Timestamp"].dt.strftime("%Y-%m-%d %H:00"))["Count"].sum().reset_index()
            charts["saves_all"] = {
                "labels": df_saves_per_hour["Timestamp"].tolist(),
                "data": df_saves_per_hour["Count"].tolist()
            }
            logging.info(f"📊 Przygotowano dane saves_all: {len(charts['saves_all']['labels'])} etykiet, {len(charts['saves_all']['data'])} wartości")

            df_saves["Day"] = df_saves["Timestamp"].dt.date
            for day in df_saves["Day"].unique():
                df_day = df_saves[df_saves["Day"] == day].copy()
                if not df_day.empty:
                    df_day["Hour"] = df_day["Timestamp"].dt.strftime("%H:00")
                    saves_per_hour = df_day.groupby("Hour")["Count"].sum().reset_index()
                    charts[f"saves_{day}"] = {
                        "labels": saves_per_hour["Hour"].tolist(),
                        "data": saves_per_hour["Count"].tolist()
                    }
                    logging.info(f"📊 Przygotowano dane saves_{day}: {len(charts[f'saves_{day}']['labels'])} etykiet, {len(charts[f'saves_{day}']['data'])} wartości")
        else:
            logging.info("⚠️ Nie znaleziono zapisów gry.")
            charts["saves_all"] = {"labels": [], "data": []}
        return df_saves, charts
    except Exception as e:
        with open(ERROR_LOG, "a", encoding="utf-8") as f:
            f.write(f"{datetime.now()}: Błąd w handle_saves: {e}\n{traceback.format_exc()}\n")
        logging.error(f"❌ Błąd w handle_saves: {e}")
        return pd.DataFrame(), {}

# Monitorowanie i predykcje
def monitor_and_predict(warnings):
    charts = {}
    try:
        if not warnings.empty:
            df_warn = pd.DataFrame(warnings)
            df_warn["Timestamp"] = pd.to_datetime(df_warn["Timestamp"], errors="coerce")
            df_warn = df_warn.dropna(subset=["Timestamp"]).sort_values("Timestamp")
            df_warn["DateTime"] = df_warn["Timestamp"].dt.strftime("%Y-%m-%d %H:00")
            warn_per_hour = df_warn.groupby("DateTime").size().reset_index(name="Count")
            
            if len(warn_per_hour) >= 2:
                charts["warnings_per_hour"] = {
                    "labels": warn_per_hour["DateTime"].tolist(),
                    "data": warn_per_hour["Count"].tolist()
                }
                logging.info(f"📊 Przygotowano dane warnings_per_hour: {len(charts['warnings_per_hour']['labels'])} etykiet, {len(charts['warnings_per_hour']['data'])} wartości")

                df_warn["Day"] = df_warn["Timestamp"].dt.date
                for day in df_warn["Day"].unique():
                    df_day = df_warn[df_warn["Day"] == day].copy()
                    if not df_day.empty:
                        df_day["Hour"] = df_day["Timestamp"].dt.strftime("%H:00")
                        warn_per_hour_day = df_day.groupby("Hour").size().reset_index(name="Count")
                        charts[f"warnings_per_hour_{day}"] = {
                            "labels": warn_per_hour_day["Hour"].tolist(),
                            "data": warn_per_hour_day["Count"].tolist()
                        }
                        logging.info(f"📊 Przygotowano dane warnings_per_hour_{day}: {len(charts[f'warnings_per_hour_{day}']['labels'])} etykiet, {len(charts[f'warnings_per_hour_{day}']['data'])} wartości")
            else:
                logging.info("⚠️ Za mało danych do predykcji.")
                charts["warnings_per_hour"] = {"labels": [], "data": []}
        else:
            logging.info("✅ Brak ostrzeżeń - serwer stabilny.")
            charts["warnings_per_hour"] = {"labels": [], "data": []}
        return charts
    except Exception as e:
        with open(ERROR_LOG, "a", encoding="utf-8") as f:
            f.write(f"{datetime.now()}: Błąd w monitor_and_predict: {e}\n{traceback.format_exc()}\n")
        logging.error(f"❌ Błąd w monitor_and_predict: {e}")
        return {}

# Eksport danych (tylko do pamięci, bez zapisu do plików)
def export_data(events, sessions_df):
    try:
        df = pd.DataFrame(events)
        df["Timestamp"] = pd.to_datetime(df["Timestamp"], errors="coerce")
        return df
    except Exception as e:
        with open(ERROR_LOG, "a", encoding="utf-8") as f:
            f.write(f"{datetime.now()}: Błąd w export_data: {e}\n{traceback.format_exc()}\n")
        logging.error(f"❌ Błąd w export_data: {e}")
        return pd.DataFrame()

# Eksport modów z problemami (tylko do pamięci, bez zapisu do plików)
def export_mod_issues(df, mod_issues):
    charts = {}
    try:
        if mod_issues:
            charts["mod_issues"] = {
                "labels": list(mod_issues.keys()),
                "data": list(mod_issues.values())
            }
            logging.info(f"📊 Przygotowano dane mod_issues: {len(charts['mod_issues']['labels'])} etykiet, {len(charts['mod_issues']['data'])} wartości")
        else:
            charts["mod_issues"] = {"labels": [], "data": []}
            logging.info("⚠️ Brak problemów z modami do wykresu.")
    except Exception as e:
        with open(ERROR_LOG, "a", encoding="utf-8") as f:
            f.write(f"{datetime.now()}: Błąd w export_mod_issues: {e}\n{traceback.format_exc()}\n")
        logging.error(f"❌ Błąd w export_mod_issues: {e}")
    return charts

# Generowanie wykresów
def generate_charts(df, sessions_df, admin_cmds):
    charts = {}
    try:
        if df is None or df.empty:
            logging.info("⚠️ Brak danych do wykresów.")
            charts["event_types"] = {"labels": [], "data": []}
            charts["events_per_hour"] = {"labels": [], "data": []}
            charts["admin_commands"] = {"labels": [], "data": []}
            return charts

        event_counts = df["LineType"].value_counts().reset_index()
        event_counts.columns = ["LineType", "Count"]
        charts["event_types"] = {
            "labels": event_counts["LineType"].tolist(),
            "data": event_counts["Count"].tolist()
        }
        logging.info(f"📊 Przygotowano dane event_types: {len(charts['event_types']['labels'])} etykiet, {len(charts['event_types']['data'])} wartości")

        if not df["Timestamp"].dropna().empty:
            event_per_hour = df.groupby(df["Timestamp"].dt.strftime("%Y-%m-%d %H:00"))["EventType"].count().reset_index(name="Count")
            charts["events_per_hour"] = {
                "labels": event_per_hour["Timestamp"].tolist(),
                "data": event_per_hour["Count"].tolist()
            }
            logging.info(f"📊 Przygotowano dane events_per_hour: {len(charts['events_per_hour']['labels'])} etykiet, {len(charts['events_per_hour']['data'])} wartości")

            df["Day"] = df["Timestamp"].dt.date
            for day in df["Day"].unique():
                df_day = df[df["Day"] == day].copy()
                if not df_day.empty:
                    event_per_hour_day = df_day.groupby(df_day["Timestamp"].dt.strftime("%H:00"))["EventType"].count().reset_index(name="Count")
                    charts[f"events_per_hour_{day}"] = {
                        "labels": event_per_hour_day["Timestamp"].tolist(),
                        "data": event_per_hour_day["Count"].tolist()
                    }
                    logging.info(f"📊 Przygotowano dane events_per_hour_{day}: {len(charts[f'events_per_hour_{day}']['labels'])} etykiet, {len(charts[f'events_per_hour_{day}']['data'])} wartości")
        else:
            charts["events_per_hour"] = {"labels": [], "data": []}
            logging.info("⚠️ Brak znaczników czasowych dla events_per_hour.")

        if not admin_cmds.empty:
            cmd_counts = admin_cmds["Details"].apply(lambda x: parse_details(x).get("Command", parse_details(x).get("Message", parse_details(x).get("User", "Unknown")))).value_counts()
            charts["admin_commands"] = {
                "labels": cmd_counts.index.tolist(),
                "data": cmd_counts.values.tolist()
            }
            logging.info(f"📊 Przygotowano dane admin_commands: {len(charts['admin_commands']['labels'])} etykiet, {len(charts['admin_commands']['data'])} wartości")
        else:
            charts["admin_commands"] = {"labels": [], "data": []}
            logging.info("⚠️ Brak komend admina do wykresu.")

        return charts
    except Exception as e:
        with open(ERROR_LOG, "a", encoding="utf-8") as f:
            f.write(f"{datetime.now()}: Błąd w generate_charts: {e}\n{traceback.format_exc()}\n")
        logging.error(f"❌ Błąd w generate_charts: {e}")
        return {}

# Podsumowanie błędów
def summarize_errors(errors):
    if errors.empty:
        return []
    error_counts = errors["Details"].apply(lambda x: parse_details(x).get("Message", "Unknown")).value_counts().head(5)
    return [{"Message": msg, "Count": count} for msg, count in error_counts.items()]

# Podsumowanie ostrzeżeń
def summarize_warnings(warnings):
    if warnings.empty:
        return []
    warning_counts = warnings["Details"].apply(lambda x: parse_details(x).get("Message", "Unknown")).value_counts().head(5)
    return [{"Message": msg, "Count": count} for msg, count in warning_counts.items()]

# Podsumowanie sesji graczy
def summarize_sessions(sessions_df):
    if sessions_df.empty:
        return []
    total_duration = sessions_df.groupby("Player")["Duration"].sum().reset_index().sort_values("Duration", ascending=False)
    return total_duration.to_dict('records')

def generate_html_report(
    events,
    event_counts,
    errors,
    warnings,
    warning_types,
    mod_issues,
    sessions_df,
    admin_cmds,
    save_charts,
    warning_charts,
    other_charts
):
    try:
        df = pd.DataFrame(events)
        report_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        # Bezpieczne records
        def safe_records(df_obj, cols):
            try:
                return df_obj[cols].to_dict('records')
            except Exception:
                try:
                    return df_obj.to_dict('records')
                except Exception:
                    return []

        errors_data = safe_records(errors, ["Timestamp", "EventType", "Details"])
        warnings_data = safe_records(warnings, ["Timestamp", "EventType", "Details"])
        sessions_data = safe_records(sessions_df, ["Player", "Start", "End", "Duration"])
        admin_data = safe_records(admin_cmds, ["Timestamp", "EventType", "Details"])

        try:
            mods_data = df[df["EventType"] == "mod_load"][["Details"]].to_dict('records')
        except Exception:
            mods_data = []

        # Podsumowania
        try:
            errors_summary = summarize_errors(errors)
        except Exception:
            errors_summary = []
        try:
            warnings_summary = summarize_warnings(warnings)
        except Exception:
            warnings_summary = []
        try:
            sessions_summary = summarize_sessions(sessions_df)
        except Exception:
            sessions_summary = []

        # Admin summary
        def extract_admin_action(val):
            try:
                d = parse_details(val)
                return d.get("Command", d.get("Message", d.get("User", "Unknown")))
            except Exception:
                return "Unknown"

        try:
            admin_summary = admin_cmds["Details"].apply(extract_admin_action).value_counts().to_dict()
        except Exception:
            admin_summary = {}

        # FILTROWANIE: usuń mod_issues z other_charts
        cleaned_other_charts = {}
        for k, v in (other_charts or {}).items():
            if k == "mod_issues" or k.startswith("mod_issues"):
                continue
            cleaned_other_charts[k] = v

        # Usuń kumulacyjne
        filtered_save_charts = {k: v for k, v in (save_charts or {}).items() if k != "saves_all"}
        filtered_warning_charts = {k: v for k, v in (warning_charts or {}).items() if k != "warnings_per_hour"}
        filtered_other_charts = {}
        banned_other = {"events_per_hour"}
        allowed_aggregates = {"event_types", "admin_commands"}
        for k, v in cleaned_other_charts.items():
            if k in banned_other:
                continue
            if k.startswith("events_per_hour_") or k in allowed_aggregates:
                filtered_other_charts[k] = v
            else:
                if isinstance(v, dict):
                    filtered_other_charts[k] = v

        # Przygotowanie danych do wykresów
        charts_data = {
            "other_charts": filtered_other_charts or {},
            "save_charts": filtered_save_charts or {},
            "warning_charts": filtered_warning_charts or {},
            "sessions_charts": (
                {"sessions_total": {
                    "labels": [row.get("Player", "") for row in sessions_summary],
                    "data": [row.get("Duration", 0) for row in sessions_summary],
                    "type": "bar",
                    "horizontal": True
                }} if sessions_summary else {}
            ),
            "admin_charts": (
                {"admin_actions": {
                    "labels": list(admin_summary.keys()),
                    "data": list(admin_summary.values()),
                    "type": "bar",
                    "horizontal": True
                }} if admin_summary else {}
            ),
            "mod_issues": (
                {"mod_issues": {
                    "labels": list(mod_issues.keys()),
                    "data": list(mod_issues.values()),
                    "type": "bar",
                    "horizontal": True
                }} if mod_issues else {}
            )
        }

        # JavaScript
        javascript_code = """
document.addEventListener('DOMContentLoaded', () => {
    const toggleButton = document.getElementById('theme-toggle');
    if (toggleButton) {
        toggleButton.addEventListener('click', () => {
            document.body.classList.toggle('dark');
            localStorage.setItem('theme', document.body.classList.contains('dark') ? 'dark' : 'light');
        });
        if (localStorage.getItem('theme') === 'dark') {
            document.body.classList.add('dark');
        }
    }

    document.querySelectorAll('.sortable th').forEach(header => {
        header.addEventListener('click', () => {
            const table = header.closest('table');
            const index = Array.from(header.parentElement.children).indexOf(header);
            const tbody = table.querySelector('tbody');
            if (!tbody) return;
            const rows = Array.from(tbody.rows);
            const isAscending = header.classList.contains('asc');
            rows.sort((a, b) => {
                const aText = a.cells[index]?.textContent?.trim() ?? '';
                const bText = b.cells[index]?.textContent?.trim() ?? '';
                return isAscending
                    ? bText.localeCompare(aText, undefined, { numeric: true })
                    : aText.localeCompare(bText, undefined, { numeric: true });
            });
            header.classList.toggle('asc');
            tbody.innerHTML = '';
            rows.forEach(row => tbody.appendChild(row));
        });
    });

    document.querySelectorAll('a[href^="#"]').forEach(anchor => {
        anchor.addEventListener('click', (e) => {
            e.preventDefault();
            const target = document.querySelector(anchor.getAttribute('href'));
            if (!target) return;
            target.scrollIntoView({ behavior: 'smooth' });
        });
    });

    if (typeof Chart === 'undefined') {
        const errorDiv = document.createElement('div');
        errorDiv.className = 'text-center text-red-500 dark:text-red-400 p-4';
        errorDiv.textContent = 'Błąd: Nie załadowano Chart.js. Otwórz raport przez serwer HTTP (np. python -m http.server).';
        document.querySelector('.container').prepend(errorDiv);
        return;
    }

    const chartsDataElement = document.getElementById('charts-data');
    if (!chartsDataElement) return;
    let chartsData;
    try {
        chartsData = JSON.parse(chartsDataElement.textContent);
    } catch (e) {
        const errorDiv = document.createElement('div');
        errorDiv.className = 'text-center text-red-500 dark:text-red-400 p-4';
        errorDiv.textContent = 'Błąd: Nie udało się sparsować danych wykresów.';
        document.querySelector('.container').prepend(errorDiv);
        return;
    }

    const styleMap = {
        other_charts:   { title: 'Inne zdarzenia', color: '#3b82f6', bg: 'rgba(59,130,246,0.35)', defaultType: 'bar', horizontal: false },
        save_charts:    { title: 'Zapisy gry (dzień)', color: '#10b981', bg: 'rgba(16,185,129,0.35)', defaultType: 'line', horizontal: false },
        warning_charts: { title: 'Ostrzeżenia (dzień)', color: '#ef4444', bg: 'rgba(239,68,68,0.35)',  defaultType: 'line', horizontal: false },
        sessions_charts:{ title: 'Sesje graczy',   color: '#6366f1', bg: 'rgba(99,102,241,0.35)', defaultType: 'bar', horizontal: true },
        admin_charts:   { title: 'Akcje admina',   color: '#14b8a6', bg: 'rgba(20,184,166,0.35)', defaultType: 'bar', horizontal: true },
        mod_issues:     { title: 'Problemy z modami', color: '#8b5cf6', bg: 'rgba(139,92,246,0.35)', defaultType: 'bar', horizontal: true },
    };

    function computeHeight(labels, horizontal) {
        if (!Array.isArray(labels)) return 420;
        const base = horizontal ? 90 : 120;
        const perLabel = horizontal ? 28 : 20;
        const h = base + labels.length * perLabel;
        return Math.max(380, Math.min(1000, h));
    }

    Object.entries(chartsData).forEach(([sectionKey, group]) => {
        const container = document.getElementById(sectionKey);
        if (!container) return;

        const style = styleMap[sectionKey] || { title: sectionKey, color: '#374151', bg: 'rgba(55,65,81,0.3)', defaultType: 'bar', horizontal: false };

        if (!group || Object.keys(group).length === 0) {
            const ph = document.createElement('div');
            ph.className = 'chart-container flex items-center justify-center';
            ph.style.minHeight = '160px';
            ph.innerHTML = `<div class="text-center"><div style="font-weight:600;margin-bottom:6px">${style.title}</div><div style="color:#6b7280">Obecnie brak danych</div></div>`;
            container.appendChild(ph);
            return;
        }

        Object.entries(group).forEach(([subKey, chartData]) => {
            const labels = chartData?.labels ?? [];
            const data = chartData?.data ?? [];
            const type = chartData?.type || style.defaultType;
            const horizontal = chartData?.horizontal ?? style.horizontal;

            if (!Array.isArray(labels) || labels.length === 0 || !Array.isArray(data) || data.length === 0) {
                const ph = document.createElement('div');
                ph.className = 'chart-container flex items-center justify-center';
                ph.style.minHeight = '160px';
                ph.innerHTML = `<div class="text-center"><div style="font-weight:600;margin-bottom:6px">${style.title}: ${subKey}</div><div style="color:#6b7280">Obecnie brak danych</div></div>`;
                container.appendChild(ph);
                return;
            }

            const height = computeHeight(labels, horizontal);
            const wrapper = document.createElement('div');
            wrapper.className = 'chart-container';
            wrapper.style.height = `${height}px`;
            container.appendChild(wrapper);

            const canvas = document.createElement('canvas');
            wrapper.appendChild(canvas);

            try {
                new Chart(canvas, {
                    type,
                    data: {
                        labels,
                        datasets: [{
                            label: `${style.title}: ${subKey}`,
                            data,
                            borderColor: style.color,
                            backgroundColor: style.bg,
                            fill: type === 'line',
                            tension: type === 'line' ? 0.35 : 0
                        }]
                    },
                    options: {
                        responsive: true,
                        maintainAspectRatio: false,
                        layout: {
                            padding: { left: horizontal ? 24 : 8, right: 12, top: 12, bottom: 18 }
                        },
                        plugins: {
                            legend: { display: true, labels: { boxWidth: 18, boxHeight: 12 } },
                            title: { display: true, text: `${style.title}: ${subKey}`, font: { size: 14 } }
                        },
                        scales: {
                            x: {
                                ticks: { maxRotation: horizontal ? 0 : 45, minRotation: horizontal ? 0 : 45, autoSkip: false, font: { size: 12 } },
                                title: { display: true, text: horizontal ? 'Wartość' : 'Godzina', font: { size: 12 } }
                            },
                            y: {
                                ticks: { autoSkip: false, font: { size: 12 } },
                                title: { display: true, text: horizontal ? 'Kategoria' : 'Liczba', font: { size: 12 } }
                            }
                        },
                        indexAxis: horizontal ? 'y' : 'x'
                    }
                });
            } catch (e) {
                const ph = document.createElement('div');
                ph.className = 'chart-container flex items personally-center justify-center';
                ph.style.minHeight = '160px';
                ph.innerHTML = `<div class="text-center"><div style="font-weight:600;margin-bottom:6px">Błąd renderu: ${style.title}: ${subKey}</div><div style="color:#ef4444">Sprawdź logi</div></div>`;
                container.appendChild(ph);
                console.error(`Błąd renderowania wykresu ${sectionKey}:${subKey}`, e);
            }
        });
    });
});
"""

        # HTML
        html_content = f"""<!DOCTYPE html>
<html lang="pl">
<head>
    <meta charset="UTF-8">
    <title>Raport FS25</title>
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <script src="https://cdn.tailwindcss.com"></script>
    <script src="https://cdn.jsdelivr.net/npm/chart.js@4.5.0/dist/chart.umd.min.js"></script>
    <style>
        .chart-container {{
            position: relative;
            width: 100%;
            overflow: hidden;
            margin-bottom: 1.5rem;
            padding: 0.5rem 1rem;
            background-color: #fff;
            border-radius: 0.5rem;
            box-shadow: 0 4px 6px rgba(0,0,0,0.08);
            display: block;
        }}
        .chart-container.flex {{ display:flex; align-items:center; justify-content:center; }}
        .dark .chart-container {{ background-color: #111827 }}
        canvas {{ width: 100%; height: 100%; display: block; }}
        .table-header {{ cursor: pointer; }}
    </style>
</head>
<body class="bg-gray-100 dark:bg-gray-900 text-gray-900 dark:text-gray-100 transition-colors duration-300">
    <nav class="bg-blue-600 dark:bg-blue-800 p-4">
        <div class="container mx-auto flex justify-between items-center">
            <h1 class="text-xl font-bold text-white">Raport FS25</h1>
            <div class="space-x-4">
                <a href="#summary" class="text-white hover:underline">Podsumowanie</a>
                <a href="#charts" class="text-white hover:underline">Wykresy</a>
                <a href="#errors" class="text-white hover:underline">Błędy</a>
                <a href="#warnings" class="text-white hover:underline">Ostrzeżenia</a>
                <a href="#sessions" class="text-white hover:underline">Sesje Graczy</a>
                <a href="#admin" class="text-white hover:underline">Akcje Admina</a>
                <a href="#mods" class="text-white hover:underline">Mody</a>
                <a href="#mod-issues" class="text-white hover:underline">Problemy z modami</a>
                <button id="theme-toggle" class="px-4 py-2 bg-blue-500 text-white rounded hover:bg-blue-600">Przełącz motyw</button>
            </div>
        </div>
    </nav>

    <div class="container mx-auto p-6">
        <p class="mb-6">Wygenerowano: {report_time}</p>

        <!-- Podsumowanie -->
        <section id="summary" class="mb-8 fade-in">
            <h2 class="text-2xl font-semibold mb-4">Podsumowanie</h2>
            <div class="grid grid-cols-1 md:grid-cols-3 gap-4">
                <div class="bg-white dark:bg-gray-800 p-4 rounded shadow">
                    <h3 class="text-lg font-medium">Liczba zdarzeń</h3>
                    <p class="text-2xl">{len(events)}</p>
                </div>
                <div class="bg-white dark:bg-gray-800 p-4 rounded shadow">
                    <h3 class="text-lg font-medium">Błędy</h3>
                    <p class="text-2xl">{len(errors)}</p>
                </div>
                <div class="bg-white dark:bg-gray-800 p-4 rounded shadow">
                    <h3 class="text-lg font-medium">Ostrzeżenia</h3>
                    <p class="text-2xl">{len(warnings)}</p>
                </div>
                <div class="bg-white dark:bg-gray-800 p-4 rounded shadow">
                    <h3 class="text-lg font-medium">Mody</h3>
                    <p class="text-2xl">{len(df[df['EventType']=='mod_load'])}</p>
                </div>
                <div class="bg-white dark:bg-gray-800 p-4 rounded shadow">
                    <h3 class="text-lg font-medium">DLC</h3>
                    <p class="text-2xl">{len(df[df['EventType']=='dlc_load'])}</p>
                </div>
                <div class="bg-white dark:bg-gray-800 p-4 rounded shadow">
                    <h3 class="text-lg font-medium">Sesje graczy</h3>
                    <p class="text-2xl">{len(sessions_df)}</p>
                </div>
            </div>
        </section>

        <!-- Wykresy -->
        <section id="charts" class="mb-8 fade-in">
            <h2 class="text-2xl font-semibold mb-4">Wykresy</h2>
            <div class="grid grid-cols-1 md:grid-cols-2 gap-4">
                <div id="other_charts" class="chart-container"></div>
                <div id="save_charts" class="chart-container"></div>
                <div id="warning_charts" class="chart-container"></div>
                <div id="sessions_charts" class="chart-container"></div>
                <div id="admin_charts" class="chart-container"></div>
                <div id="mod_issues" class="chart-container"></div>
            </div>
            <script id="charts-data" type="application/json">
                {json.dumps(charts_data, ensure_ascii=False)}
            </script>
        </section>

        <!-- Błędy -->
        <section id="errors" class="mb-8 fade-in">
            <h2 class="text-2xl font-semibold mb-4">Błędy</h2>
            <table class="w-full bg-white dark:bg-gray-800 rounded shadow mb-4 sortable">
                <thead>
                    <tr class="bg-gray-200 dark:bg-gray-700">
                        <th class="p-2 table-header">Wiadomość</th>
                        <th class="p-2 table-header">Liczba</th>
                    </tr>
                </thead>
                <tbody>
                    {''.join([f'<tr><td class="p-2">{row.get("Message","")}</td><td class="p-2">{row.get("Count",0)}</td></tr>' for row in errors_summary])}
                </tbody>
            </table>
            <details class="mb-4">
                <summary class="cursor-pointer text-blue-600 dark:text-blue-400">Pokaż pełne dane błędów</summary>
                <table class="w-full bg-white dark:bg-gray-800 rounded shadow sortable">
                    <thead>
                        <tr class="bg-gray-200 dark:bg-gray-700">
                            <th class="p-2 table-header">Timestamp</th>
                            <th class="p-2 table-header">Typ zdarzenia</th>
                            <th class="p-2 table-header">Szczegóły</th>
                        </tr>
                    </thead>
                    <tbody>
                        {''.join([f'<tr><td class="p-2">{str(row.get("Timestamp",""))}</td><td class="p-2">{row.get("EventType","")}</td><td class="p-2">{str(row.get("Details",""))}</td></tr>' for row in errors_data])}
                    </tbody>
                </table>
            </details>
        </section>

        <!-- Ostrzeżenia -->
        <section id="warnings" class="mb-8 fade-in">
            <h2 class="text-2xl font-semibold mb-4">Ostrzeżenia</h2>
            <table class="w-full bg-white dark:bg-gray-800 rounded shadow mb-4 sortable">
                <thead>
                    <tr class="bg-gray-200 dark:bg-gray-700">
                        <th class="p-2 table-header">Wiadomość</th>
                        <th class="p-2 table-header">Liczba</th>
                    </tr>
                </thead>
                <tbody>
                    {''.join([f'<tr><td class="p-2">{row.get("Message","")}</td><td class="p-2">{row.get("Count",0)}</td></tr>' for row in warnings_summary])}
                </tbody>
            </table>
            <details class="mb-4">
                <summary class="cursor-pointer text-blue-600 dark:text-blue-400">Pokaż pełne dane ostrzeżeń</summary>
                <table class="w-full bg-white dark:bg-gray-800 rounded shadow sortable">
                    <thead>
                        <tr class="bg-gray-200 dark:bg-gray-700">
                            <th class="p-2 table-header">Timestamp</th>
                            <th class="p-2 table-header">Typ zdarzenia</th>
                            <th class="p-2 table-header">Szczegóły</th>
                        </tr>
                    </thead>
                    <tbody>
                        {''.join([f'<tr><td class="p-2">{str(row.get("Timestamp",""))}</td><td class="p-2">{row.get("EventType","")}</td><td class="p-2">{str(row.get("Details",""))}</td></tr>' for row in warnings_data])}
                    </tbody>
                </table>
            </details>
        </section>

        <!-- Sesje graczy -->
        <section id="sessions" class="mb-8 fade-in">
            <h2 class="text-2xl font-semibold mb-4">Sesje graczy</h2>
            <table class="w-full bg-white dark:bg-gray-800 rounded shadow mb-4">
                <thead>
                    <tr class="bg-gray-200 dark:bg-gray-700">
                        <th class="p-2 table-header">Gracz</th>
                        <th class="p-2 table-header">Całkowity czas (min)</th>
                    </tr>
                </thead>
                <tbody>
                    {''.join([f'<tr><td class="p-2">{row.get("Player","")}</td><td class="p-2">{row.get("Duration",0):.2f}</td></tr>' for row in sessions_summary])}
                </tbody>
            </table>
            <details class="mb-4">
                <summary class="cursor-pointer text-blue-600 dark:text-blue-400">Pokaż pełne dane sesji</summary>
                <div class="overflow-x-auto mt-3">
                    <table class="w-full bg-white dark:bg-gray-800 rounded shadow sortable">
                        <thead>
                            <tr class="bg-gray-200 dark:bg-gray-700">
                                <th class="p-2 table-header">Gracz</th>
                                <th class="p-2 table-header">Start</th>
                                <th class="p-2 table-header">Koniec</th>
                                <th class="p-2 table-header">Czas trwania (min)</th>
                            </tr>
                        </thead>
                        <tbody>
                            {''.join([f'<tr><td class="p-2">{row.get("Player","")}</td><td class="p-2">{(row.get("Start") or "")}</td><td class="p-2">{(row.get("End") or "")}</td><td class="p-2">{float(row.get("Duration",0)):.2f}</td></tr>' for row in sessions_data])}
                        </tbody>
                    </table>
                </div>
            </details>
        </section>

        <!-- Admin -->
        <section id="admin" class="mb-8 fade-in">
            <h2 class="text-2xl font-semibold mb-4">Akcje admina</h2>
            <table class="w-full bg-white dark:bg-gray-800 rounded shadow mb-4 sortable">
                <thead>
                    <tr class="bg-gray-200 dark:bg-gray-700">
                        <th class="p-2 table-header">Akcja</th>
                        <th class="p-2 table-header">Liczba</th>
                    </tr>
                </thead>
                <tbody>
                    {''.join([f'<tr><td class="p-2">{action}</td><td class="p-2">{count}</td></tr>' for action, count in admin_summary.items()])}
                </tbody>
            </table>
        </section>

        <!-- Mody -->
        <section id="mods" class="mb-8 fade-in">
            <h2 class="text-2xl font-semibold mb-4">Mody</h2>
            <table class="w-full bg-white dark:bg-gray-800 rounded shadow mb-4 sortable">
                <thead>
                    <tr class="bg-gray-200 dark:bg-gray-700">
                        <th class="p-2 table-header">Nazwa</th>
                        <th class="p-2 table-header">Hash</th>
                        <th class="p-2 table-header">Wersja</th>
                    </tr>
                </thead>
                <tbody>
                    {''.join([f'<tr><td class="p-2">{(row.get("Details") or {}).get("Name","")}</td><td class="p-2">{(row.get("Details") or {}).get("Hash","")}</td><td class="p-2">{(row.get("Details") or {}).get("Version","")}</td></tr>' for row in mods_data])}
                </tbody>
            </table>
        </section>

        <!-- Problemy z modami -->
        <section id="mod-issues" class="mb-8 fade-in">
            <h2 class="text-2xl font-semibold mb-4">Problemy z modami</h2>
            <table class="w-full bg-white dark:bg-gray-800 rounded shadow sortable">
                <thead>
                    <tr class="bg-gray-200 dark:bg-gray-700">
                        <th class="p-2 table-header">Mod</th>
                        <th class="p-2 table-header">Liczba problemów</th>
                    </tr>
                </thead>
                <tbody>
                    {''.join([f'<tr><td class="p-2">{mod}</td><td class="p-2">{count}</td></tr>' for mod, count in (mod_issues or {}).items()])}
                </tbody>
            </table>
        </section>

        <footer class="text-center text-gray-600 dark:text-gray-400">
            <p>Wygenerowano przez logs_analyzer.py</p>
        </footer>
    </div>

    <script>
        {javascript_code}
    </script>
</body>
</html>
"""

        # Zapisanie raportu
        report_path = os.path.join(REPORT_DIR, "index.html")
        with open(report_path, "w", encoding="utf-8") as f:
            f.write(html_content)
        logging.info(f"📄 Raport HTML zapisany jako {report_path}")
    except Exception as e:
        try:
            with open(ERROR_LOG, "a", encoding="utf-8") as f:
                f.write(f"{datetime.now()}: Błąd w generate_html_report: {e}\n{traceback.format_exc()}\n")
        except Exception as e2:
            logging.error(f"Nie udało się dopisać do ERROR_LOG: {e2}")
        logging.error(f"❌ Błąd w generate_html_report: {e}")

# Główna funkcja

def main():
    try:
        print("✅ Skrypt uruchomiony — zaczynam analizę...")
        download_logs(FTP_DIR)
        download_logs(FTP_DIR2)
        events, event_counts = analyze_logs()
        errors, warnings, warning_types, mod_issues, sessions_df, admin_cmds = detect_errors_and_stats(events)
        df_saves, save_charts = handle_saves(events)
        warning_charts = monitor_and_predict(warnings)
        other_charts = generate_charts(pd.DataFrame(events), sessions_df, admin_cmds)
        df = export_data(events, sessions_df)
        mod_charts = export_mod_issues(df, mod_issues)
        other_charts.update(mod_charts)
        generate_html_report(events, event_counts, errors, warnings, warning_types, mod_issues, sessions_df, admin_cmds, save_charts, warning_charts, other_charts)
        logging.info("✅ Analiza zakończona pomyślnie.")
    except Exception as e:
        with open(ERROR_LOG, "a", encoding="utf-8") as f:
            f.write(f"{datetime.now()}: Błąd w main: {e}\n{traceback.format_exc()}\n")
        logging.error(f"❌ Błąd w main: {e}")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print("❌ Błąd podczas działania skryptu:")
        traceback.print_exc()
        sys.exit(1)