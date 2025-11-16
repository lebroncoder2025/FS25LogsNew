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
from zoneinfo import ZoneInfo

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

# Dynamiczny plik logów (każde uruchomienie w osobnym pliku z timestampem)
START_TIME_STR = datetime.now(ZoneInfo("Europe/Warsaw")).strftime("%Y-%m-%d_%H-%M-%S")
SOFTWARE_LOG_FILE = os.path.join("logs", f"software_logs_{START_TIME_STR}.log")

# Archive or rename any legacy 'software_logs.log' to avoid very large files in the repo
LEGACY_LOG = os.path.join("logs", "software_logs.log")
if os.path.exists(LEGACY_LOG):
    try:
        # Only archive if the file is substantial to avoid cluttering
        size = os.path.getsize(LEGACY_LOG)
        if size > 1024 * 1024:  # if larger than 1MB
            archived_name = os.path.join("logs", f"software_logs_archived_{START_TIME_STR}.log")
            os.replace(LEGACY_LOG, archived_name)
            print(f"Archiving old log to {archived_name}")
        else:
            # Remove small legacy log if present
            os.remove(LEGACY_LOG)
    except Exception:
        # Ignore archive issues — proceed with new logging
        pass

# Konfiguracja logging
logging.basicConfig(
    filename=SOFTWARE_LOG_FILE,
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

# Funkcja do ładowania konfiguracji z config.json z fallbackiem na zmienne środowiskowe
def load_ftp_config():
    config_file = "config.json"
    ftp_config = {}
    
    # Próbujemy wczytać z config.json
    if os.path.exists(config_file):
        try:
            with open(config_file, "r", encoding="utf-8") as f:
                ftp_config = json.load(f)
            logging.info("📋 Konfiguracja FTP wczytana z pliku config.json")
        except Exception as e:
            logging.warning(f"⚠️ Błąd przy wczytywaniu config.json: {e}. Używam zmiennych GitHub Actions.")
    else:
        logging.warning("⚠️ Plik config.json nie znaleziony. Używam zmiennych GitHub Actions.")
    
    # Fallback na zmienne środowiskowe
    FTP_HOST = ftp_config.get("FTP_HOST") or os.environ.get("FTP_HOST")
    FTP_PORT = ftp_config.get("FTP_PORT") or int(os.environ.get("FTP_PORT", "21"))
    FTP_USER = ftp_config.get("FTP_USER") or os.environ.get("FTP_USER")
    FTP_PASS = ftp_config.get("FTP_PASS") or os.environ.get("FTP_PASS")
    FTP_DIR = ftp_config.get("FTP_DIR") or os.environ.get("FTP_DIR")
    FTP_DIR2 = ftp_config.get("FTP_DIR2") or os.environ.get("FTP_DIR2")
    FTP_DIR3 = ftp_config.get("FTP_DIR3") or os.environ.get("FTP_DIR3")
    
    return {
        "FTP_HOST": FTP_HOST,
        "FTP_PORT": int(FTP_PORT) if FTP_PORT else 21,
        "FTP_USER": FTP_USER,
        "FTP_PASS": FTP_PASS,
        "FTP_DIR": FTP_DIR,
        "FTP_DIR2": FTP_DIR2,
        "FTP_DIR3": FTP_DIR3
    }

# Ładujemy konfigurację
config = load_ftp_config()
FTP_HOST = config["FTP_HOST"]
FTP_PORT = config["FTP_PORT"]
FTP_USER = config["FTP_USER"]
FTP_PASS = config["FTP_PASS"]
FTP_DIR = config["FTP_DIR"]
FTP_DIR2 = config["FTP_DIR2"]
FTP_DIR3 = config["FTP_DIR3"]

print("✅ Konfiguracja zakończona — startuję analizę...")

# Wzorce do parsowania
TIMESTAMP = re.compile(r"(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3})")
EVENTS = {
    "player_connected": r"(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3})\s+([^\s].*?)\s+(joined the game)",
    "player_disconnected": r"(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3})\s+([^\s].*?)\s+(lost connection to the game|left the game)",
    "executed_command": r"(?:Executed command|Admin command|Command): (\w+)\s*(.*)",
    "admin_action": r"ADMIN: (.*)",
    "lua_error": r"Error: Running LUA method '(\w+)'. (.*)",
    "script_error": r"Script error in (\w+): (.*)",
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
    "vehicle_action": r"Successfully (?:added|set fill unit|moved) (.+)",
    "farm_action": r"Info: (?:Updated field|Moved|Successfully)(.+)",
    "pallet_action": r"Info: Adding pallet",
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
        with ftplib.FTP(timeout=10) as ftp:
            ftp.connect(FTP_HOST, FTP_PORT, timeout=10)
            ftp.login(FTP_USER, FTP_PASS)
            ftp.cwd(DIR)
            entries = []
            ftp.retrlines("MLSD", entries.append)
            files = [line.split(";")[-1].strip() for line in entries if line.endswith(".txt") or line.endswith(".log")]
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
                    try:
                        with open(local_path, "wb") as f:
                            ftp.retrbinary(f"RETR " + filename, f.write)
                        logging.info(f"✅ Pobrano: {filename}")
                    except Exception as file_error:
                        logging.warning(f"⚠️ Błąd podczas pobierania {filename}: {file_error}")
                        if os.path.exists(local_path):
                            try:
                                os.remove(local_path)
                            except:
                                pass
    except Exception as e:
        with open(ERROR_LOG, "a", encoding="utf-8") as f:
            f.write(f"{datetime.now()}: Błąd FTP: {e}\n{traceback.format_exc()}\n")
        logging.error(f"❌ Błąd FTP: {e}")

# Parsowanie linii
def parse_line(line):
    try:
        line = line.encode('utf-8', errors='ignore').decode('utf-8').strip()
        if not line or len(line) < 5:
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

        # Skip regex matching for extremely long lines (regex DOS protection)
        matched = False
        if len(line) > 10000:
            entry["EventType"] = "other"
            entry["Details"]["Message"] = line[:500] + "..." if len(line) > 500 else line
            return entry
        
        # Quick pre-check: skip regex if line doesn't contain common keywords
        # Map keywords to specific event types for faster matching
        keyword_map = {
            "joined": ["player_connected"],
            "left": ["player_disconnected"],
            "connection": ["player_disconnected"],
            "disconnected": ["player_disconnected"],
            "player": ["player_connected", "player_disconnected"],
            "error": ["lua_error", "script_error", "error", "network_decrypt_error"],
            "warning": ["warning_stream", "memory_warning", "duplicate_l10n", "mod_warning", "network_unknown_target", "warning"],
            "mod": ["mod_load", "mod_warning", "duplicate_l10n"],
            "dlc": ["dlc_load"],
            "load": ["file_load", "dlc_load", "mod_load"],
            "admin": ["admin_action", "executed_command", "master_login"],
            "save": ["save_game"],
            "lua": ["lua_error"],
        }
        
        # Determine which events to check based on line content
        events_to_check = set()
        for kw, etypes in keyword_map.items():
            if kw in line.lower():
                events_to_check.update(etypes)
        
        # If no keywords matched, check a minimal set of common events
        if not events_to_check:
            events_to_check = set(["system_info", "direct_storage", "value_line", "info_add", "forestry_helper", "density_map", "real_dirt_color", "vehicle_action", "farm_action", "pallet_action"])
        
        for etype, pattern in EVENTS.items():
            try:
                # Skip regex if event not in our pre-filtered set
                if etype not in events_to_check:
                    continue
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
                        elif etype == "script_error":
                            entry["Details"]["Method"] = match.group(1)
                            entry["Details"]["Message"] = match.group(2)
                        elif etype == "vehicle_action":
                            entry["Details"]["Action"] = match.group(1).strip()
                        elif etype == "farm_action":
                            entry["Details"]["Action"] = match.group(1).strip()
                        elif etype == "pallet_action":
                            entry["Details"]["Message"] = "Adding pallet"
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
        for fname in sorted(os.listdir(LOG_DIR)):
            if fname.endswith(".txt") or fname.endswith(".log"):
                logging.info(f"🔍 Analizuję: {fname}")
                file_events = 0
                try:
                    with open(os.path.join(LOG_DIR, fname), "r", encoding="utf-8", errors="replace") as f:
                        lines = f.readlines()
                        total_lines += len(lines)
                        
                        # IMPORTANT: Analyze ALL lines completely for data integrity
                        for line in lines:
                            parsed = parse_line(line)
                            if parsed:
                                events.append(parsed)
                                event_counts[parsed["EventType"]] += 1
                                file_events += 1
                            else:
                                unparsed_lines += 1
                except Exception as file_err:
                    logging.warning(f"⚠️ Błąd podczas analizy {fname}: {file_err}")
                    unparsed_lines += len(lines) if 'lines' in locals() else 0
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
def get_server_uptime_info(events):
    """Oblicza rzeczywisty czas startu serwera i uptime od ostatniego restartu"""
    try:
        if not events:
            return "Brak danych", "Nieznany"
        
        df = pd.DataFrame(events)
        
        # Konwertuj Timestamp do datetime
        if 'Timestamp' in df.columns:
            df['Timestamp'] = pd.to_datetime(df['Timestamp'], errors='coerce')
        
        if df.empty or df['Timestamp'].isna().all():
            return "Brak danych", "Nieznany"
        
        # Usuń wiersze bez Timestamp
        df = df.dropna(subset=['Timestamp'])
        
        if df.empty:
            return "Brak danych", "Nieznany"
        
        # WAŻNE: Szukaj Giants Engine Runtime aby znaleźć ostatni RESTART
        # To pojawia się na POCZĄTKU każdego restartu serwera
        engine_runtime_events = df[df['RawLine'].str.contains('GIANTS Engine Runtime', case=False, na=False)]
        
        if not engine_runtime_events.empty:
            # OSTATNI Engine Runtime = ostatni restart serwera!
            last_restart = engine_runtime_events['Timestamp'].max()
            logging.info(f"🔧 Found GIANTS Engine Runtime event at {last_restart}")
        else:
            # Fallback: użyj WSZYSTKICH logów ale PODZIEL NA SESJE
            # Szukaj "Loading mods" lub "Initializing" - to oznacza nowy startup
            startup_pattern = r'Loading mods|Starting server|server startup|initializing|engine startup|main system'
            startup_events = df[df['RawLine'].str.contains(startup_pattern, case=False, na=False, regex=True)]
            
            if not startup_events.empty:
                last_restart = startup_events['Timestamp'].max()
                logging.info(f"🔧 Found startup indicator event at {last_restart}")
            else:
                # Ultimate fallback: użyj FIRST event z OSTATNIEGO DNIA (server może być 24/7 ale z restartami)
                # Szukaj wszystkich ŚCIEŻEK logów i RÓŻNIC czasowych
                df_sorted = df.sort_values('Timestamp')
                
                # Analiza - jeśli jest DUŻA LUKA między eventami (>1h), to prawdopodobnie restart
                df_sorted['time_diff'] = df_sorted['Timestamp'].diff()
                big_gaps = df_sorted[df_sorted['time_diff'] > pd.Timedelta(hours=1)]
                
                if not big_gaps.empty:
                    # Użyj EVENT PO ostatniej dużej luce
                    last_restart = big_gaps.iloc[-1]['Timestamp']
                    logging.info(f"🔧 Detected server restart via time gap at {last_restart}")
                else:
                    # Jeśli brak restartów - użyj PIERWSZEGO eventu
                    last_restart = df_sorted.iloc[0]['Timestamp']
                    logging.info(f"🔧 No restart detected, using first event at {last_restart}")
        
        # Oblicz uptime
        now = pd.Timestamp.now()
        uptime_delta = now - last_restart
        
        # Formatuj uptime
        days = uptime_delta.days
        hours = uptime_delta.seconds // 3600
        minutes = (uptime_delta.seconds % 3600) // 60
        
        if days > 0:
            uptime_str = f"{days}d {hours}h {minutes}m"
        elif hours > 0:
            uptime_str = f"{hours}h {minutes}m"
        else:
            uptime_str = f"{minutes}m"
        
        # Formatuj czas startu
        start_time_str = last_restart.strftime("%Y-%m-%d %H:%M:%S")
        
        logging.info(f"🖥️ Server startup: {start_time_str}, Uptime: {uptime_str}")
        
        return uptime_str, start_time_str
    except Exception as e:
        logging.error(f"❌ Błąd przy obliczaniu uptime: {e}")
        import traceback
        logging.error(traceback.format_exc())
        return "Błąd", "Nieznany"

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
        logging.info(f"🔍 admin_player_stats: Total events: {len(df)}")
        df["Timestamp"] = pd.to_datetime(df["Timestamp"], errors="coerce")

        connects = df[df["EventType"] == "player_connected"].sort_values("Timestamp")
        disconnects = df[df["EventType"] == "player_disconnected"].sort_values("Timestamp")
        logging.info(f"🔍 admin_player_stats: player_connected events: {len(connects)}, player_disconnected: {len(disconnects)}")
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

# Pobieranie ostatnich logów serwera
def get_server_startup_logs():
    """Pobiera ostatnie logi uruchomienia serwera z plików logów"""
    server_logs = []
    try:
        log_cache_dir = os.path.join(os.path.dirname(__file__), 'log_cache')
        if not os.path.exists(log_cache_dir):
            return []
        log_files = sorted([f for f in os.listdir(log_cache_dir) if f.endswith('.txt')])
        for log_file in log_files[-5:]:  # Check last 5 log files
            file_path = os.path.join(log_cache_dir, log_file)
            with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                lines = f.readlines()
                for i, line in enumerate(lines):
                    # Look for server startup indicators
                    if any(x in line for x in ['GIANTS Engine Runtime', 'Application:', 'Started', 'Loading', 'Initializing', 'Main System']):
                        timestamp = log_file.replace('log_', '').replace('.txt', '').replace('_', ' ')
                        message = line.strip()[:150]
                        if message and len(message) > 10:
                            server_logs.append({
                                'Timestamp': timestamp,
                                'Message': message
                            })
        return server_logs[-20:]  # Keep only last 20 entries
    except Exception as e:
        logging.error(f"Błąd przy pobieraniu logów serwera: {e}")
        return []

# Podsumowanie błędów
def summarize_errors(errors):
    if errors.empty:
        return []
    error_counts = errors["Details"].apply(lambda x: parse_details(x).get("Message", "Unknown")).value_counts()
    return [{"Message": msg, "Count": count} for msg, count in error_counts.items()]

# Podsumowanie ostrzeżeń
def summarize_warnings(warnings):
    if warnings.empty:
        return []
    warning_counts = warnings["Details"].apply(lambda x: parse_details(x).get("Message", "Unknown")).value_counts()
    return [{"Message": msg, "Count": count} for msg, count in warning_counts.items()]

# Podsumowanie sesji graczy
def summarize_sessions(sessions_df):
    if sessions_df.empty:
        logging.info("⚠️ summarize_sessions: sessions_df jest pusty!")
        return []
    total_duration = sessions_df.groupby("Player")["Duration"].sum().reset_index().sort_values("Duration", ascending=False)
    logging.info(f"✓ summarize_sessions: Created {len(total_duration)} sessions summary")
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
        report_time = datetime.now(ZoneInfo("Europe/Warsaw")).strftime("%Y-%m-%d %H:%M:%S")

        # Przygotowanie danych
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
        
        # Konwertuj Timestamp objects do strings (aby sortowanie działało w JS)
        def format_timestamp_records(records):
            formatted = []
            for row in records:
                formatted_row = row.copy()
                # Konwertuj WSZYSTKIE kolumny które zawierają Timestamp obiekty
                for key in formatted_row:
                    ts = formatted_row[key]
                    # Sprawdź czy to Timestamp
                    if hasattr(ts, 'strftime') or pd.isna(ts):
                        try:
                            if pd.isna(ts):
                                formatted_row[key] = ''
                            elif hasattr(ts, 'strftime'):
                                formatted_row[key] = ts.strftime('%Y-%m-%d %H:%M:%S')
                            else:
                                formatted_row[key] = str(ts)
                        except Exception:
                            formatted_row[key] = str(ts) if ts is not None else ''
                formatted.append(formatted_row)
            return formatted
        
        errors_data = format_timestamp_records(errors_data)
        warnings_data = format_timestamp_records(warnings_data)
        sessions_data = safe_records(sessions_df, ["Player", "Start", "End", "Duration"])
        
        # Konwertuj sessions data - Start i End do strings
        sessions_data = format_timestamp_records(sessions_data)
        
        admin_data = safe_records(admin_cmds, ["Timestamp", "EventType", "Details"])

        try:
            mods_data = df[df["EventType"] == "mod_load"][["Details"]].to_dict('records')
        except Exception:
            mods_data = []

        errors_summary = summarize_errors(errors) if not errors.empty else []
        warnings_summary = summarize_warnings(warnings) if not warnings.empty else []
        sessions_summary = summarize_sessions(sessions_df) if not sessions_df.empty else []

        def extract_admin_action(val):
            try:
                d = parse_details(val)
                return d.get("Command", d.get("Message", d.get("User", "Unknown")))
            except Exception:
                return "Unknown"

        admin_summary = admin_cmds["Details"].apply(extract_admin_action).value_counts().to_dict() if not admin_cmds.empty else {}

        # Generowanie HTML dla wszystkich błędów i ostrzeżeń
        html_errors = ''.join([f'<tr><td>{row.get("Timestamp","")}</td><td>{row.get("Details","")}</td><td>{row.get("EventType","")}</td></tr>' for row in errors_data])
        html_warnings = ''.join([f'<tr><td>{row.get("Timestamp","")}</td><td>{row.get("Details","")}</td><td>{row.get("EventType","")}</td></tr>' for row in warnings_data])
        
        # Pobieranie ostatnich logów serwera
        server_logs = get_server_startup_logs()
        server_logs_html = ''.join([f'<tr><td>{log.get("Timestamp","")}</td><td>{log.get("Message","")}</td></tr>' for log in server_logs])
        
        # Obliczanie rzeczywistego uptime i czasu startu serwera
        uptime_str, start_time_str = get_server_uptime_info(events)

        # Przygotowanie danych do wykresów
        cleaned_other_charts = {k: v for k, v in (other_charts or {}).items() if k != "mod_issues" and not k.startswith("mod_issues")}
        filtered_save_charts = {k: v for k, v in (save_charts or {}).items() if k != "saves_all"}
        filtered_warning_charts = {k: v for k, v in (warning_charts or {}).items() if k != "warnings_per_hour"}
        
        charts_data = json.dumps({
            "other_charts": filtered_save_charts or {},
            "warning_charts": filtered_warning_charts or {},
            "sessions": {"sessions_total": {
                "labels": [row.get("Player", "") for row in sessions_summary],
                "data": [row.get("Duration", 0) for row in sessions_summary],
            }} if sessions_summary else {},
            "admin": {"admin_actions": {
                "labels": list(admin_summary.keys()),
                "data": list(admin_summary.values()),
            }} if admin_summary else {},
            "mod_issues_data": {
                "labels": list(mod_issues.keys()),
                "data": list(mod_issues.values()),
            } if mod_issues else {}
        }, ensure_ascii=False)

        # Generowanie HTML z CSS i JS inline - nowy motyw FS25
        html_content = f"""<!DOCTYPE html>
<html lang="pl">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>🚜 FS25 Analyzer - Raport Serwera</title>
    <script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0"></script>
    <style>
        * {{ margin: 0; padding: 0; box-sizing: border-box; }}
        
        body {{
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            background: linear-gradient(135deg, #1a3a08 0%, #0d1f04 100%);
            background-attachment: fixed;
            color: #f0f0f0;
            line-height: 1.6;
            transition: background 0.3s, color 0.3s;
            position: relative;
            padding: 0;
            margin: 0;
        }}
        
        body::before {{
            content: '🚜 🌾 🚜 🌾 🚜 🌾 🚜 🌾';
            position: fixed;
            top: 0;
            left: 0;
            right: 0;
            bottom: 0;
            font-size: 150px;
            opacity: 0.02;
            pointer-events: none;
            white-space: pre-wrap;
            overflow: hidden;
            z-index: -1;
            line-height: 1.2;
        }}
        body.dark-mode {{
            background: linear-gradient(135deg, #0d1b04 0%, #050a02 100%);
            color: #e8e8e8;
        }}
        
        .container {{ max-width: 1400px; margin: 0 auto; padding: 0 20px; }}
        
        /* Navbar - FS25 Style */
        nav {{
            background: linear-gradient(90deg, #8B7355 0%, #D4A574 50%, #8B7355 100%);
            box-shadow: 0 4px 15px rgba(0,0,0,0.3), inset 0 1px 0 rgba(255,255,255,0.1);
            padding: 0;
            position: sticky;
            top: 0;
            z-index: 1000;
            border-bottom: 3px solid #FFD700;
        }}
        .nav-container {{
            max-width: 1400px;
            margin: 0 auto;
            padding: 15px 20px;
            display: flex;
            justify-content: space-between;
            align-items: center;
            flex-wrap: wrap;
            gap: 20px;
        }}
        .nav-title {{
            font-size: 28px;
            font-weight: bold;
            display: flex;
            align-items: center;
            gap: 10px;
            color: #0d1b04;
            text-shadow: 1px 1px 2px rgba(255,255,255,0.3);
        }}
        .nav-tabs {{
            display: flex;
            gap: 5px;
            flex-wrap: wrap;
            align-items: center;
        }}
        .nav-tab {{
            color: #0d1b04;
            cursor: pointer;
            padding: 10px 16px;
            border-radius: 4px;
            font-size: 14px;
            font-weight: 500;
            transition: all 0.3s;
            background: rgba(255,255,255,0.1);
            border: 1px solid transparent;
        }}
        .nav-tab:hover, .nav-tab.active {{
            background: #FFD700;
            color: #0d1b04;
            border: 1px solid #FFA500;
            transform: scale(1.05);
        }}
        .theme-toggle {{
            background: rgba(255,255,255,0.15);
            border: 1px solid rgba(255,255,255,0.3);
            color: #0d1b04;
            padding: 10px 14px;
            border-radius: 4px;
            cursor: pointer;
            font-size: 18px;
            transition: all 0.3s ease;
            font-weight: bold;
        }}
        .theme-toggle:hover {{ 
            background: rgba(255,255,255,0.3);
            transform: scale(1.1);
            box-shadow: 0 0 10px rgba(255,255,255,0.2);
        }}
        .theme-toggle:active {{
            transform: scale(0.95);
        }}
        
        /* Content */
        main {{ padding: 40px 20px; }}
        .section {{
            display: none;
            opacity: 0;
        }}
        .section.active {{ 
            display: block;
            animation: fadeInSlide 0.5s ease-out forwards;
        }}
        @keyframes fadeInSlide {{ 
            from {{ 
                opacity: 0; 
                transform: translateY(20px); 
            }}
            to {{ 
                opacity: 1; 
                transform: translateY(0); 
            }}
        }}
        
        h1 {{
            font-size: 36px;
            margin-bottom: 30px;
            color: #FFD700;
            text-shadow: 2px 2px 4px rgba(0,0,0,0.5);
            border-bottom: 2px solid #FFD700;
            padding-bottom: 15px;
            animation: fadeInDown 0.6s ease-out;
        }}
        h2 {{
            font-size: 24px;
            margin: 25px 0 15px 0;
            color: #FFD700;
            border-left: 4px solid #FFD700;
            padding-left: 15px;
            animation: fadeInLeft 0.6s ease-out 0.1s both;
        }}
        @keyframes fadeInDown {{
            from {{
                opacity: 0;
                transform: translateY(-20px);
            }}
            to {{
                opacity: 1;
                transform: translateY(0);
            }}
        }}
        @keyframes fadeInLeft {{
            from {{
                opacity: 0;
                transform: translateX(-20px);
            }}
            to {{
                opacity: 1;
                transform: translateX(0);
            }}
        }}
        }}
        
        /* Cards */
        .stats-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 20px;
            margin-bottom: 40px;
        }}
        .stat-card {{
            background: linear-gradient(135deg, #2d5016 0%, #1a3a0a 100%);
            border-radius: 8px;
            padding: 20px;
            box-shadow: 0 4px 12px rgba(0,0,0,0.3), inset 0 1px 0 rgba(255,255,255,0.1);
            border-left: 4px solid #FFD700;
            transition: all 0.3s;
            position: relative;
            overflow: hidden;
            animation: fadeInUp 0.6s ease-out;
        }}
        .stat-card:nth-child(2) {{ animation-delay: 0.05s; }}
        .stat-card:nth-child(3) {{ animation-delay: 0.1s; }}
        .stat-card:nth-child(4) {{ animation-delay: 0.15s; }}
        .stat-card::before {{
            content: '';
            position: absolute;
            top: 0;
            left: 0;
            right: 0;
            height: 2px;
            background: linear-gradient(90deg, #FFD700, #FFA500, transparent);
        }}
        .stat-card:hover {{
            transform: translateY(-8px);
            box-shadow: 0 8px 20px rgba(255,215,0,0.3);
            border-left-color: #FFA500;
        }}
        .stat-label {{ font-size: 11px; color: #b8c5a6; text-transform: uppercase; letter-spacing: 1px; margin-bottom: 10px; }}
        .stat-value {{ font-size: 32px; font-weight: bold; color: #FFD700; text-shadow: 1px 1px 2px rgba(0,0,0,0.5); }}
        
        /* Tables */
        table {{
            width: 100%;
            border-collapse: collapse;
            background: linear-gradient(135deg, rgba(45,80,22,0.8), rgba(26,58,10,0.8));
            border-radius: 8px;
            overflow: hidden;
            box-shadow: 0 4px 12px rgba(0,0,0,0.3);
            margin-bottom: 20px;
            border: 1px solid rgba(255,215,0,0.2);
            animation: fadeInUp 0.6s ease-out;
        }}
        @keyframes fadeInUp {{
            from {{
                opacity: 0;
                transform: translateY(20px);
            }}
            to {{
                opacity: 1;
                transform: translateY(0);
            }}
        }}
        thead {{
            background: linear-gradient(90deg, #8B7355 0%, #D4A574 100%);
            font-weight: 600;
            color: #0d1b04;
            position: sticky;
            top: 0;
            z-index: 10;
        }}
        th, td {{
            padding: 14px 15px;
            text-align: left;
            border-bottom: 1px solid rgba(255,215,0,0.15);
            transition: background 0.2s;
        }}
        th {{ cursor: pointer; user-select: none; font-weight: 700; }}
        th:hover {{ background: #D4A574; }}
        tbody tr {{
            transition: all 0.2s ease;
        }}
        tbody tr:hover {{ 
            background: rgba(255,215,0,0.15);
            transform: scale(1.005);
            box-shadow: inset 0 0 10px rgba(255,215,0,0.1);
        }}
        td {{ vertical-align: middle; }}
        
        /* Charts */
        .chart-container {{
            background: linear-gradient(135deg, rgba(45,80,22,0.6), rgba(26,58,10,0.6));
            border-radius: 8px;
            padding: 10px;
            margin-bottom: 10px;
            box-shadow: 0 4px 12px rgba(0,0,0,0.3);
            border: 1px solid rgba(255,215,0,0.15);
            max-height: 200px;
            overflow-y: auto;
            animation: fadeInUp 0.7s ease-out 0.15s both;
        }}
        .chart-container canvas {{
            max-height: 180px !important;
        }}
        .chart-title {{
            color: #FFD700;
            margin-bottom: 15px;
            font-weight: 600;
            font-size: 16px;
            text-shadow: 1px 1px 2px rgba(0,0,0,0.3);
        }}
        
        /* Search/Filter */
        .search-box {{
            margin-bottom: 20px;
        }}
        .search-box input {{
            padding: 12px 15px;
            border: 2px solid rgba(255,215,0,0.3);
            border-radius: 4px;
            width: 100%;
            max-width: 400px;
            font-size: 14px;
            background: rgba(45,80,22,0.5);
            color: #f0f0f0;
            transition: all 0.3s ease;
            font-family: inherit;
        }}
        .search-box input:focus {{
            outline: none;
            border-color: #FFD700;
            background: rgba(45,80,22,0.8);
            box-shadow: 0 0 15px rgba(255,215,0,0.4), inset 0 0 10px rgba(255,215,0,0.05);
            transform: scale(1.02);
        }}
        .search-box input::placeholder {{
            color: #8B7355;
        }}
        
        /* Summary Stats */
        .summary-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(250px, 1fr));
            gap: 20px;
            margin-bottom: 30px;
        }}
        .summary-box {{
            background: linear-gradient(135deg, rgba(139,115,85,0.3), rgba(212,165,116,0.2));
            border: 2px solid rgba(255,215,0,0.3);
            border-radius: 8px;
            padding: 20px;
            text-align: center;
            transition: all 0.3s ease;
            position: relative;
            overflow: hidden;
            animation: fadeInUp 0.6s ease-out;
        }}
        .summary-box:nth-child(2) {{ animation-delay: 0.05s; }}
        .summary-box:nth-child(3) {{ animation-delay: 0.1s; }}
        .summary-box:nth-child(4) {{ animation-delay: 0.15s; }}
        .summary-box:nth-child(5) {{ animation-delay: 0.2s; }}
        .summary-box:nth-child(6) {{ animation-delay: 0.25s; }}
        .summary-box::before {{
            content: '';
            position: absolute;
            top: -50%;
            left: -50%;
            width: 200%;
            height: 200%;
            background: radial-gradient(circle, rgba(255,215,0,0.1) 0%, transparent 70%);
            opacity: 0;
            transition: opacity 0.3s ease;
        }}
        .summary-box:hover {{
            transform: translateY(-8px) scale(1.05);
            border-color: rgba(255,215,0,0.6);
            box-shadow: 0 8px 20px rgba(255,215,0,0.2), inset 0 0 10px rgba(255,215,0,0.05);
        }}
        .summary-box:hover::before {{
            opacity: 1;
        }}
        .summary-label {{ color: #b8c5a6; font-size: 13px; text-transform: uppercase; margin-bottom: 8px; letter-spacing: 0.5px; }}
        .summary-value {{ font-size: 28px; font-weight: bold; color: #FFD700; text-shadow: 1px 1px 2px rgba(0,0,0,0.3); }}
        
        /* Footer */
        footer {{
            text-align: center;
            padding: 30px 20px;
            color: #8B7355;
            border-top: 2px solid #FFD700;
            margin-top: 50px;
            background: linear-gradient(180deg, rgba(45,80,22,0.3) 0%, rgba(13,27,4,0.3) 100%);
            font-size: 13px;
        }}
        footer a {{
            color: #FFD700;
            text-decoration: none;
            transition: all 0.2s;
        }}
        footer a:hover {{
            color: #FFA500;
            text-decoration: underline;
        }}
        
        /* Icons */
        .icon {{ font-size: 20px; margin-right: 8px; }}
        
        /* Buttons & Links */
        button {{
            cursor: pointer;
            border: none;
            outline: none;
        }}
        a {{
            color: #FFD700;
            text-decoration: none;
            transition: all 0.2s;
        }}
        a:hover {{
            color: #FFA500;
            text-decoration: underline;
        }}
        
        /* Loading state */
        .loading {{
            opacity: 0.6;
            pointer-events: none;
        }}
        
        /* Scrollbar styling */
        ::-webkit-scrollbar {{
            width: 8px;
            height: 8px;
        }}
        ::-webkit-scrollbar-track {{
            background: rgba(45,80,22,0.3);
        }}
        ::-webkit-scrollbar-thumb {{
            background: #FFD700;
            border-radius: 4px;
        }}
        ::-webkit-scrollbar-thumb:hover {{
            background: #FFA500;
        }}
        
        /* Expandable Lists */
        .expandable {{
            cursor: pointer;
            user-select: none;
            padding: 12px 15px;
            background: linear-gradient(135deg, #2d2d2d, #1a1a1a);
            border-left: 3px solid #FFD700;
            margin-bottom: 5px;
            border-radius: 4px;
            display: flex;
            justify-content: space-between;
            align-items: center;
            transition: all 0.3s;
            border: 1px solid rgba(255,215,0,0.1);
        }}
        
        .expandable:hover {{
            background: linear-gradient(135deg, #3d3d3d, #2a2a2a);
            box-shadow: 0 2px 8px rgba(255,215,0,0.2);
            border-left-color: #FFA500;
        }}
        
        .expandable-icon {{
            font-size: 0.8em;
            margin-left: 10px;
            transition: transform 0.3s;
            color: #FFD700;
            font-weight: bold;
        }}
        
        .expandable-icon.active {{
            transform: rotate(90deg);
        }}
        
        .expandable-content {{
            display: none;
            background: rgba(30,30,30,0.9);
            padding: 15px;
            margin-bottom: 8px;
            border-radius: 4px;
            border-left: 3px solid #FF6B6B;
            border: 1px solid rgba(255,107,107,0.2);
            animation: slideDown 0.3s ease-out;
        }}
        
        .expandable-content.active {{
            display: block;
        }}
        
        @keyframes slideDown {{
            from {{
                opacity: 0;
                transform: translateY(-10px);
            }}
            to {{
                opacity: 1;
                transform: translateY(0);
            }}
        }}
        
        .detail-row {{
            padding: 8px 0;
            border-bottom: 1px solid rgba(255,215,0,0.1);
            color: #e0e0e0;
            font-size: 0.9em;
        }}
        
        .detail-row:last-child {{
            border-bottom: none;
        }}
        
        .detail-label {{
            color: #FFD700;
            font-weight: bold;
            margin-right: 10px;
            display: inline-block;
            min-width: 120px;
        }}
        
        .detail-row code {{
            background: rgba(0,0,0,0.3);
            padding: 4px 8px;
            border-radius: 3px;
            color: #80FF00;
            font-family: 'Courier New', monospace;
            font-size: 0.85em;
            word-break: break-all;
        }}
        
        @media (max-width: 1200px) {{
            .container {{ padding: 0 15px; }}
            .summary-grid {{ grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 15px; }}
            main {{ padding: 30px 15px; }}
        }}
        
        @media (max-width: 768px) {{
            .nav-container {{ flex-direction: column; align-items: stretch; gap: 10px; }}
            .nav-tabs {{ width: 100%; flex-wrap: wrap; justify-content: center; }}
            .nav-title {{ text-align: center; width: 100%; font-size: 22px; }}
            .stats-grid {{ grid-template-columns: 1fr; }}
            .summary-grid {{ grid-template-columns: 1fr; gap: 12px; }}
            h1 {{ font-size: 24px; margin-bottom: 15px; }}
            h2 {{ font-size: 20px; }}
            table {{ font-size: 13px; }}
            th, td {{ padding: 10px 8px; }}
            .search-box input {{ max-width: 100%; }}
            .chart-container {{ padding: 15px; }}
            main {{ padding: 20px 10px; }}
            .container {{ padding: 0 10px; }}
            .nav-tab {{ padding: 8px 12px; font-size: 13px; flex: 1; min-width: 80px; text-align: center; }}
            .theme-toggle {{ padding: 8px 12px; min-width: auto; }}
        }}
        
        @media (max-width: 480px) {{
            .nav-container {{ gap: 5px; }}
            .nav-tabs {{ gap: 3px; }}
            .nav-title {{ font-size: 18px; }}
            .nav-tab {{ padding: 6px 8px; font-size: 11px; min-width: 65px; }}
            .theme-toggle {{ padding: 6px 8px; font-size: 16px; }}
            h1 {{ font-size: 20px; }}
            h2 {{ font-size: 17px; }}
            table {{ font-size: 11px; }}
            th, td {{ padding: 8px 5px; }}
            .summary-box {{ padding: 15px; }}
            .summary-value {{ font-size: 24px; }}
            .summary-label {{ font-size: 11px; }}
            .chart-container {{ padding: 12px; margin-bottom: 15px; }}
            main {{ padding: 15px 8px; }}
            .search-box input {{ padding: 10px 12px; font-size: 13px; }}
        }}
    </style>
</head>
<body>
    <nav>
        <div class="nav-container">
            <div class="nav-title">🚜 FS25 Analyzer</div>
            <div class="nav-tabs">
                <div class="nav-tab active" onclick="switchTab('dashboard')">📊 Dashboard</div>
                <div class="nav-tab" onclick="switchTab('charts')">📈 Wykresy</div>
                <div class="nav-tab" onclick="switchTab('errors')">❌ Błędy</div>
                <div class="nav-tab" onclick="switchTab('warnings')">⚠️ Ostrzeżenia</div>
                <div class="nav-tab" onclick="switchTab('mods')">🧩 Mody</div>
                <div class="nav-tab" onclick="switchTab('sessions')">👥 Sesje</div>
                <div class="nav-tab" onclick="switchTab('server')">🖥️ Serwer</div>
                <button class="theme-toggle" onclick="toggleTheme()">🌙</button>
            </div>
        </div>
    </nav>

    <main>
        <!-- Dashboard -->
        <div id="dashboard" class="section active">
            <h1>📊 Dashboard</h1>
            <div class="summary-grid">
                <div class="summary-box">
                    <div class="summary-label">🌐 Zdarzeń razem</div>
                    <div class="summary-value">{len(events)}</div>
                </div>
                <div class="summary-box">
                    <div class="summary-label">⏱️ Błędy</div>
                    <div class="summary-value">{len(errors)}</div>
                </div>
                <div class="summary-box">
                    <div class="summary-label">⚠️ Ostrzeżenia</div>
                    <div class="summary-value">{len(warnings)}</div>
                </div>
                <div class="summary-box">
                    <div class="summary-label">👥 Sesje graczy</div>
                    <div class="summary-value">{len(sessions_df)}</div>
                </div>
                <div class="summary-box">
                    <div class="summary-label">🧩 Mody</div>
                    <div class="summary-value">{len(df[df['EventType']=='mod_load'])}</div>
                </div>
                <div class="summary-box">
                    <div class="summary-label">📦 DLC</div>
                    <div class="summary-value">{len(df[df['EventType']=='dlc_load'])}</div>
                </div>
            </div>
            
            <h2>📋 Podsumowanie</h2>
            <div class="summary-grid">
                <div class="summary-box">
                    <div class="summary-label">💾 Zapisów gry</div>
                    <div class="summary-value">{len(df[df['EventType']=='save_game'])}</div>
                </div>
                <div class="summary-box">
                    <div class="summary-label">⚙️ Akcji admina</div>
                    <div class="summary-value">{len(admin_cmds)}</div>
                </div>
                <div class="summary-box">
                    <div class="summary-label">🔧 Akcji pojazdu</div>
                    <div class="summary-value">{len(df[df['EventType']=='vehicle_action'])}</div>
                </div>
                <div class="summary-box">
                    <div class="summary-label">🌾 Akcji farmy</div>
                    <div class="summary-value">{len(df[df['EventType']=='farm_action'])}</div>
                </div>
            </div>
            
            <p style="margin-top: 20px; color: #b8c5a6;"><strong>📅 Wygenerowano:</strong> {report_time}</p>
        </div>

        <!-- Charts -->
        <div id="charts" class="section">
            <h1>📈 Wykresy</h1>
            <div id="chartsContainer"></div>
        </div>

        <!-- Errors -->
        <div id="errors" class="section">
            <h1>❌ Błędy</h1>
            <div class="search-box">
                <input type="text" id="errorSearch" placeholder="Szukaj błędu..." onkeyup="filterTable('errorSearch', 'errorTable')">
            </div>
            
            <div style="margin: 20px 0; display: flex; gap: 10px;">
                <button onclick="toggleTable('errorTable', 'allErrorTable', this)" style="padding: 10px 20px; background: #FFD700; color: #0d1b04; border: none; border-radius: 4px; cursor: pointer; font-weight: bold; font-size: 14px;">📊 Podsumowanie</button>
                <button onclick="toggleTable('allErrorTable', 'errorTable', this)" style="padding: 10px 20px; background: rgba(255,215,0,0.3); color: #FFD700; border: 1px solid #FFD700; border-radius: 4px; cursor: pointer; font-weight: bold; font-size: 14px;">📋 Szczegóły</button>
            </div>
            
            <h2>📊 Podsumowanie błędów (top unique - sortowalne)</h2>
            <table id="errorTable">
                <thead>
                    <tr>
                        <th onclick="sortTableByName('errorTable', 0)">📝 Wiadomość 🔻</th>
                        <th onclick="sortTableByName('errorTable', 1)">🔢 Liczba 🔻</th>
                    </tr>
                </thead>
                <tbody data-sortAsc="false">
                    {''.join([f'<tr><td>{row.get("Message","")}</td><td>{row.get("Count",0)}</td></tr>' for row in errors_summary])}
                </tbody>
            </table>

            <h2 style="display: none;" id="allErrorTableTitle">📋 Wszystkie błędy (pełna lista - sortowalne po dacie)</h2>
            <div class="search-box" style="display: none;" id="allErrorSearchBox">
                <input type="text" id="allErrorSearch" placeholder="Szukaj..." onkeyup="filterTable('allErrorSearch', 'allErrorTable')">
            </div>
            <table id="allErrorTable" style="display: none;">
                <thead>
                    <tr>
                        <th onclick="sortTableByName('allErrorTable', 0)">📅 Data/Czas 🔻</th>
                        <th onclick="sortTableByName('allErrorTable', 1)">📝 Wiadomość 🔻</th>
                        <th onclick="sortTableByName('allErrorTable', 2)">🔧 Typ 🔻</th>
                    </tr>
                </thead>
                <tbody data-sortAsc="false">
                    {html_errors}
                </tbody>
            </table>
        </div>

        <!-- Warnings -->
        <div id="warnings" class="section">
            <h1>⚠️ Ostrzeżenia</h1>
            <div class="search-box">
                <input type="text" id="warningSearch" placeholder="Szukaj ostrzeżenia..." onkeyup="filterTable('warningSearch', 'warningTable')">
            </div>
            
            <div style="margin: 20px 0; display: flex; gap: 10px;">
                <button onclick="toggleTable('warningTable', 'allWarningTable', this)" style="padding: 10px 20px; background: #FFD700; color: #0d1b04; border: none; border-radius: 4px; cursor: pointer; font-weight: bold; font-size: 14px;">📊 Podsumowanie</button>
                <button onclick="toggleTable('allWarningTable', 'warningTable', this)" style="padding: 10px 20px; background: rgba(255,215,0,0.3); color: #FFD700; border: 1px solid #FFD700; border-radius: 4px; cursor: pointer; font-weight: bold; font-size: 14px;">📋 Szczegóły</button>
            </div>
            
            <h2>📊 Podsumowanie ostrzeżeń (top unique - sortowalne)</h2>
            <table id="warningTable">
                <thead>
                    <tr>
                        <th onclick="sortTableByName('warningTable', 0)">📝 Wiadomość 🔻</th>
                        <th onclick="sortTableByName('warningTable', 1)">🔢 Liczba 🔻</th>
                    </tr>
                </thead>
                <tbody data-sortAsc="false">
                    {''.join([f'<tr><td>{row.get("Message","")}</td><td>{row.get("Count",0)}</td></tr>' for row in warnings_summary])}
                </tbody>
            </table>

            <h2 style="display: none;" id="allWarningTableTitle">📋 Wszystkie ostrzeżenia (pełna lista - sortowalne po dacie)</h2>
            <div class="search-box" style="display: none;" id="allWarningSearchBox">
                <input type="text" id="allWarningSearch" placeholder="Szukaj..." onkeyup="filterTable('allWarningSearch', 'allWarningTable')">
            </div>
            <table id="allWarningTable" style="display: none;">
                <thead>
                    <tr>
                        <th onclick="sortTableByName('allWarningTable', 0)">📅 Data/Czas 🔻</th>
                        <th onclick="sortTableByName('allWarningTable', 1)">📝 Wiadomość 🔻</th>
                        <th onclick="sortTableByName('allWarningTable', 2)">🔧 Typ 🔻</th>
                    </tr>
                </thead>
                <tbody data-sortAsc="false">
                    {html_warnings}
                </tbody>
            </table>
        </div>

        <!-- Server -->
        <div id="server" class="section">
            <h1>🖥️ Serwer</h1>
            
            <h2>📊 Podsumowanie serwera</h2>
            <div class="stats-grid">
                <div class="stat-card">
                    <div class="stat-label">Status serwera</div>
                    <div class="stat-value">✅ Aktywny</div>
                </div>
                <div class="stat-card">
                    <div class="stat-label">Czas działania</div>
                    <div class="stat-value">{uptime_str}</div>
                </div>
                <div class="stat-card">
                    <div class="stat-label">Ostatni start</div>
                    <div class="stat-value">{start_time_str}</div>
                </div>
                <div class="stat-card">
                    <div class="stat-label">Liczba graczy</div>
                    <div class="stat-value">{len(set(row.get("Player","") for row in sessions_data)) if sessions_data else 0}</div>
                </div>
                <div class="stat-card">
                    <div class="stat-label">Łączny czas sesji</div>
                    <div class="stat-value">{(sum(float(row.get("Duration",0)) for row in sessions_data) / 60 if sessions_data else 0):.0f}h</div>
                </div>
            </div>
        </div>

        <!-- Mods -->
        <div id="mods" class="section">
            <h1>🧩 Mody</h1>
            <div class="search-box">
                <input type="text" id="modSearch" placeholder="Szukaj modułu..." onkeyup="filterTable('modSearch', 'modsTable')">
            </div>
            <table id="modsTable">
                <thead>
                    <tr>
                        <th>📛 Nazwa</th>
                        <th>📌 Wersja</th>
                        <th>🔑 Hash</th>
                    </tr>
                </thead>
                <tbody data-sortAsc="false">
                    {''.join([f'<tr><td>{(row.get("Details") or {}).get("Name","")}</td><td>{(row.get("Details") or {}).get("Version","")}</td><td><code>{(row.get("Details") or {}).get("Hash","")}</code></td></tr>' for row in mods_data])}
                </tbody>
            </table>
            <h2>🚨 Problemy z modami</h2>
            <table>
                <thead>
                    <tr>
                        <th>🧩 Mod</th>
                        <th>⚠️ Liczba problemów</th>
                    </tr>
                </thead>
                <tbody data-sortAsc="false">
                    {''.join([f'<tr><td>{mod}</td><td>{count}</td></tr>' for mod, count in (mod_issues or {}).items()])}
                </tbody>
            </table>
        </div>

        <!-- Sessions -->
        <div id="sessions" class="section">
            <h1>👥 Sesje Graczy</h1>
            
            <!-- Session Summary Statistics -->
            <h2>📊 Podsumowanie sesji</h2>
            <div class="stats-grid">
                <div class="stat-card">
                    <div class="stat-label">Całkowita liczba sesji</div>
                    <div class="stat-value">{len(sessions_data)}</div>
                </div>
                <div class="stat-card">
                    <div class="stat-label">Średni czas sesji (h)</div>
                    <div class="stat-value">{(sum(float(row.get("Duration",0)) for row in sessions_data) / 60 / len(sessions_data) if sessions_data else 0):.1f}</div>
                </div>
                <div class="stat-card">
                    <div class="stat-label">Razem godzin gry</div>
                    <div class="stat-value">{(sum(float(row.get("Duration",0)) for row in sessions_data) / 60 if sessions_data else 0):.1f}</div>
                </div>
                <div class="stat-card">
                    <div class="stat-label">Unikalnych graczy</div>
                    <div class="stat-value">{len(set(row.get("Player","") for row in sessions_data))}</div>
                </div>
            </div>
            
            <!-- Player Aggregate Statistics Chart -->
            <h2>🏆 Gracze wg Czasu Gry (sortowalne)</h2>
            <table id="playerSummaryTable">
                <thead>
                    <tr>
                        <th onclick="sortTableByName('playerSummaryTable', 0)">👤 Gracz 🔻</th>
                        <th onclick="sortTableByName('playerSummaryTable', 1)">⏱️ Całkowity Czas (h) 🔻</th>
                    </tr>
                </thead>
                <tbody data-sortAsc="false">
                    {''.join([f'<tr><td>{row.get("Player","")}</td><td>{float(row.get("Duration",0))/60:.2f}</td></tr>' for row in sessions_summary])}
                </tbody>
            </table>
            
            <!-- Sessions Detail Table -->
            <h2>📋 Szczegóły sesji (sortowalne po dacie)</h2>
            <div class="search-box">
                <input type="text" id="sessionSearch" placeholder="Szukaj gracza..." onkeyup="filterTable('sessionSearch', 'sessionTable')">
            </div>
            <table id="sessionTable">
                <thead>
                    <tr>
                        <th onclick="sortTableByName('sessionTable', 0)">👤 Gracz 🔻</th>
                        <th onclick="sortTableByName('sessionTable', 1)">▶️ Start 🔻</th>
                        <th onclick="sortTableByName('sessionTable', 2)">⏹️ Koniec 🔻</th>
                        <th onclick="sortTableByName('sessionTable', 3)">⏱️ Czas (h) 🔻</th>
                        <th onclick="sortTableByName('sessionTable', 4)">📅 Data 🔻</th>
                    </tr>
                </thead>
                <tbody data-sortAsc="false">
                    {''.join([f'<tr><td>{row.get("Player","")}</td><td>{row.get("Start","")}</td><td>{row.get("End","")}</td><td>{float(row.get("Duration",0))/60:.2f}</td><td>{str(row.get("Start",""))}</td></tr>' for row in sessions_data])}
                </tbody>
            </table>
        </div>
    </main>

    <footer>
        <p>🚜 FS25 Analyzer © 2025 | Wygenerowano przez logs_analyzer.py | Raport systemowy serwera Farming Simulator 25</p>
    </footer>

    <script>
        const chartsData = {charts_data};
        
        function switchTab(tabName) {{
            document.querySelectorAll('.section').forEach(s => s.classList.remove('active'));
            document.querySelectorAll('.nav-tab').forEach(t => t.classList.remove('active'));
            document.getElementById(tabName).classList.add('active');
            event.target.classList.add('active');
            if (tabName === 'charts') initCharts();
            if (tabName === 'sessions') initPlayerSessionsChart();
        }}
        
        function toggleTheme() {{
            document.body.classList.toggle('dark-mode');
            localStorage.setItem('theme', document.body.classList.contains('dark-mode') ? 'dark' : 'light');
        }}
        
        function initCharts() {{
            const container = document.getElementById('chartsContainer');
            let chartCount = 0;
            
            // Helper function to create generic charts
            function createChart(title, type, labels, data, bgColor = 'rgba(102, 126, 234, 0.7)') {{
                if (!labels || !labels.length) return false;
                const canvas = document.createElement('canvas');
                canvas.style.marginBottom = '15px';
                canvas.style.maxHeight = '360px';
                container.appendChild(canvas);
                new Chart(canvas, {{
                    type: type,
                    data: {{
                        labels: labels,
                        datasets: [{{
                            label: title,
                            data: data,
                            backgroundColor: bgColor,
                            borderColor: bgColor.replace('0.7', '1'),
                            borderWidth: 2
                        }}]
                    }},
                    options: {{ 
                        responsive: true, 
                        indexAxis: 'y',
                        plugins: {{
                            legend: {{ labels: {{ color: '#f0f0f0' }} }},
                            title: {{ display: true, text: title, color: '#f0f0f0' }}
                        }},
                        scales: {{
                            x: {{ ticks: {{ color: '#f0f0f0' }} }},
                            y: {{ ticks: {{ color: '#f0f0f0' }} }}
                        }}
                    }}
                }});
                return true;
            }}
            
            // Session Duration Chart
            if (chartsData['sessions'] && chartsData['sessions']['sessions_total'] && chartsData['sessions']['sessions_total']['labels'].length > 0) {{
                const canvas = document.createElement('canvas');
                canvas.style.marginBottom = '15px';
                canvas.style.maxHeight = '360px';
                container.appendChild(canvas);
                new Chart(canvas, {{
                    type: 'bar',
                    data: {{
                        labels: chartsData['sessions']['sessions_total']['labels'],
                        datasets: [{{
                            label: 'Czas sesji (min)',
                            data: chartsData['sessions']['sessions_total']['data'],
                            backgroundColor: 'rgba(255, 215, 0, 0.7)',
                            borderColor: '#FFD700',
                            borderWidth: 2
                        }}]
                    }},
                    options: {{ 
                        responsive: true, 
                        indexAxis: 'y',
                        plugins: {{
                            legend: {{ labels: {{ color: '#f0f0f0' }} }},
                            title: {{ display: true, text: '👥 Czas trwania sesji graczy', color: '#f0f0f0' }}
                        }},
                        scales: {{
                            x: {{ ticks: {{ color: '#f0f0f0' }} }},
                            y: {{ ticks: {{ color: '#f0f0f0' }} }}
                        }}
                    }}
                }});
                chartCount++;
            }}
            
            // Mod Issues Chart
            if (chartsData['mod_issues_data'] && chartsData['mod_issues_data']['labels'].length > 0) {{
                const canvas = document.createElement('canvas');
                canvas.style.marginBottom = '30px';
                canvas.style.maxHeight = '360px';
                container.appendChild(canvas);
                new Chart(canvas, {{
                    type: 'bar',
                    data: {{
                        labels: chartsData['mod_issues_data']['labels'].slice(0, 15),
                        datasets: [{{
                            label: 'Liczba problemów',
                            data: chartsData['mod_issues_data']['data'].slice(0, 15),
                            backgroundColor: 'rgba(255, 100, 100, 0.7)',
                            borderColor: '#FF6464',
                            borderWidth: 2
                        }}]
                    }},
                    options: {{ 
                        responsive: true, 
                        indexAxis: 'y',
                        plugins: {{
                            legend: {{ labels: {{ color: '#f0f0f0' }} }},
                            title: {{ display: true, text: '🧩 Problemy z modami', color: '#f0f0f0' }}
                        }},
                        scales: {{
                            x: {{ ticks: {{ color: '#f0f0f0' }} }},
                            y: {{ ticks: {{ color: '#f0f0f0' }} }}
                        }}
                    }}
                }});
                chartCount++;
            }}
            
            // Admin Actions Chart
            if (chartsData['admin'] && chartsData['admin']['admin_actions'] && chartsData['admin']['admin_actions']['labels'].length > 0) {{
                const canvas = document.createElement('canvas');
                canvas.style.marginBottom = '30px';
                canvas.style.maxHeight = '360px';
                container.appendChild(canvas);
                new Chart(canvas, {{
                    type: 'doughnut',
                    data: {{
                        labels: chartsData['admin']['admin_actions']['labels'].slice(0, 10),
                        datasets: [{{
                            label: 'Akcje admina',
                            data: chartsData['admin']['admin_actions']['data'].slice(0, 10),
                            backgroundColor: [
                                '#667eea', '#764ba2', '#f093fb', '#4facfe', '#00f2fe',
                                '#43e97b', '#fa709a', '#fee140', '#30cfd0', '#330867'
                            ]
                        }}]
                    }},
                    options: {{ 
                        responsive: true,
                        plugins: {{
                            legend: {{ labels: {{ color: '#f0f0f0' }} }},
                            title: {{ display: true, text: '⚙️ Akcje administracyjne', color: '#f0f0f0' }}
                        }}
                    }}
                }});
                chartCount++;
            }}
            
            // Other charts from "other_charts"
            if (chartsData['other_charts']) {{
                for (const [chartKey, chartData] of Object.entries(chartsData['other_charts'])) {{
                    if (chartData && chartData.labels && chartData.labels.length > 0) {{
                        const canvas = document.createElement('canvas');
                        canvas.style.marginBottom = '30px';
                        canvas.style.maxHeight = '360px';
                        container.appendChild(canvas);
                        try {{
                            new Chart(canvas, {{
                                type: 'line',
                                data: {{
                                    labels: chartData.labels,
                                    datasets: [{{
                                        label: chartKey.replace(/_/g, ' '),
                                        data: chartData.data || chartData.values,
                                        backgroundColor: 'rgba(102, 126, 234, 0.2)',
                                        borderColor: '#667eea',
                                        borderWidth: 2
                                    }}]
                                }},
                                options: {{ 
                                    responsive: true,
                                    plugins: {{
                                        legend: {{ labels: {{ color: '#f0f0f0' }} }},
                                        title: {{ display: true, text: chartKey.replace(/_/g, ' '), color: '#f0f0f0' }}
                                    }},
                                    scales: {{
                                        x: {{ ticks: {{ color: '#f0f0f0' }} }},
                                        y: {{ ticks: {{ color: '#f0f0f0' }} }}
                                    }}
                                }}
                            }});
                            chartCount++;
                        }} catch(e) {{
                            console.error('Error drawing chart ' + chartKey + ':', e);
                        }}
                    }}
                }}
            }}
            
            // Warning charts
            if (chartsData['warning_charts']) {{
                for (const [chartKey, chartData] of Object.entries(chartsData['warning_charts'])) {{
                    if (chartData && chartData.labels && chartData.labels.length > 0) {{
                        const canvas = document.createElement('canvas');
                        canvas.style.marginBottom = '30px';
                        canvas.style.maxHeight = '360px';
                        container.appendChild(canvas);
                        try {{
                            new Chart(canvas, {{
                                type: 'line',
                                data: {{
                                    labels: chartData.labels,
                                    datasets: [{{
                                        label: '⚠️ ' + chartKey.replace(/_/g, ' '),
                                        data: chartData.data || chartData.values,
                                        backgroundColor: 'rgba(255, 193, 7, 0.2)',
                                        borderColor: '#FFC107',
                                        borderWidth: 2
                                    }}]
                                }},
                                options: {{ 
                                    responsive: true,
                                    plugins: {{
                                        legend: {{ labels: {{ color: '#f0f0f0' }} }},
                                        title: {{ display: true, text: '⚠️ ' + chartKey.replace(/_/g, ' '), color: '#f0f0f0' }}
                                    }},
                                    scales: {{
                                        x: {{ ticks: {{ color: '#f0f0f0' }} }},
                                        y: {{ ticks: {{ color: '#f0f0f0' }} }}
                                    }}
                                }}
                            }});
                            chartCount++;
                        }} catch(e) {{
                            console.error('Error drawing chart ' + chartKey + ':', e);
                        }}
                    }}
                }}
            }}
            
            if (chartCount === 0) {{
                container.innerHTML = '<p style="color: #999;">Brak danych do wyświetlenia na wykresach</p>';
            }}
        }}
        
        // Initialize player sessions chart on Sessions tab
        function initPlayerSessionsChart() {{
            if (chartsData['sessions'] && chartsData['sessions']['sessions_total']) {{
                const labels = chartsData['sessions']['sessions_total']['labels'];
                const dataMinutes = chartsData['sessions']['sessions_total']['data'];
                // Konwertuj minuty na godziny
                const dataHours = dataMinutes.map(m => (m / 60).toFixed(2));
                const canvas = document.getElementById('playerSessionsChart');
                if (canvas) {{
                    new Chart(canvas, {{
                        type: 'bar',
                        data: {{
                            labels: labels,
                            datasets: [{{
                                label: 'Całkowity czas gry (godziny)',
                                data: dataHours,
                                backgroundColor: 'rgba(255, 215, 0, 0.7)',
                                borderColor: '#FFD700',
                                borderWidth: 2
                            }}]
                        }},
                        options: {{ 
                            responsive: true,
                            maintainAspectRatio: true,
                            indexAxis: 'y',
                            plugins: {{
                                legend: {{ labels: {{ color: '#f0f0f0' }} }},
                                title: {{ display: false }}
                            }},
                            scales: {{
                                x: {{ ticks: {{ color: '#f0f0f0' }} }},
                                y: {{ ticks: {{ color: '#f0f0f0', font: {{ size: 10 }} }} }}
                            }}
                        }}
                    }});
                }}
            }}
        }}
        
        // Table sorting function
        function sortTable(tableId, columnIndex = 0) {{
            const table = document.getElementById(tableId);
            const tbody = table.querySelector('tbody');
            if (!tbody) return;
            
            const rows = Array.from(tbody.querySelectorAll('tr'));
            const isAscending = tbody.dataset.sortAsc === 'true';
            
            rows.sort((a, b) => {{
                const cellA = a.children[columnIndex]?.textContent.trim() || '';
                const cellB = b.children[columnIndex]?.textContent.trim() || '';
                
                // Try to parse as numbers (for time/numeric columns without date formatting)
                const numericPattern = /^-?\d+(?:\.\d+)?$/;
                if (numericPattern.test(cellA) && numericPattern.test(cellB)) {{
                    const numA = parseFloat(cellA);
                    const numB = parseFloat(cellB);
                    if (!isNaN(numA) && !isNaN(numB)) {{
                        return isAscending ? numA - numB : numB - numA;
                    }}
                }}
                
                // Sort by date if it looks like a date (YYYY-MM-DD or YYYY-MM-DD HH:MM:SS[.ffffff] format)
                const dateRegex = /\d{{4}}-\d{{2}}-\d{{2}}/;
                if (cellA.match(dateRegex) && cellB.match(dateRegex)) {{
                    // Handle "YYYY-MM-DD HH:MM:SS.ffffff" format (with microseconds)
                    let dateAStr = cellA.match(/\d{{4}}-\d{{2}}-\d{{2}}(?:\s\d{{2}}:\d{{2}}:\d{{2}}(?:\.\d{{1,6}})?)?/)[0];
                    let dateBStr = cellB.match(/\d{{4}}-\d{{2}}-\d{{2}}(?:\s\d{{2}}:\d{{2}}:\d{{2}}(?:\.\d{{1,6}})?)?/)[0];
                    
                    // Convert to ISO format for proper parsing
                    // Remove microseconds (everything after dot) and replace space with T
                    dateAStr = dateAStr.split('.')[0].replace(' ', 'T');
                    dateBStr = dateBStr.split('.')[0].replace(' ', 'T');
                    
                    const dateA = new Date(dateAStr);
                    const dateB = new Date(dateBStr);
                    
                    // Check if dates are valid
                    if (!isNaN(dateA.getTime()) && !isNaN(dateB.getTime())) {{
                        return isAscending ? dateA - dateB : dateB - dateA;
                    }}
                }}
                
                return isAscending ? cellA.localeCompare(cellB) : cellB.localeCompare(cellA);
            }});
            
            tbody.dataset.sortAsc = isAscending ? 'false' : 'true';
            rows.forEach(row => tbody.appendChild(row));
        }}
        
        // Sort table by name (for onclick handlers)
        function sortTableByName(tableId, columnIndex) {{
            sortTable(tableId, columnIndex);
        }}
        
        function filterTable(inputId, tableId) {{
            const input = document.getElementById(inputId);
            const filter = input.value.toUpperCase();
            const table = document.getElementById(tableId);
            const rows = table.querySelectorAll('tbody tr');
            rows.forEach(row => {{
                const text = row.textContent.toUpperCase();
                row.style.display = text.includes(filter) ? '' : 'none';
            }});
        }}
        
        function toggleTable(showTableId, hideTableId, buttonElement) {{
            // Hide the other table and show this one
            const showTable = document.getElementById(showTableId);
            const hideTable = document.getElementById(hideTableId);
            const parent = buttonElement.parentElement;
            const buttons = parent.querySelectorAll('button');
            
            // Handle title and search box visibility
            const baseName = hideTableId.replace('Table', ''); // e.g., 'errorTable' -> 'error'
            const titleId = baseName + 'TableTitle';
            const searchBoxId = baseName + 'SearchBox';
            const titleElement = document.getElementById(titleId);
            const searchBoxElement = document.getElementById(searchBoxId);
            
            if (hideTable) {{
                hideTable.style.display = 'none';
                // Hide associated elements
                if (titleElement) titleElement.style.display = 'none';
                if (searchBoxElement) searchBoxElement.style.display = 'none';
            }}
            if (showTable) {{
                showTable.style.display = 'table';
            }}
            
            // Update button styles
            buttons.forEach(btn => {{
                if (btn === buttonElement) {{
                    btn.style.background = '#FFD700';
                    btn.style.color = '#0d1b04';
                    btn.style.border = 'none';
                }} else {{
                    btn.style.background = 'rgba(255,215,0,0.3)';
                    btn.style.color = '#FFD700';
                    btn.style.border = '1px solid #FFD700';
                }}
            }});
        }}
        
        if (localStorage.getItem('theme') === 'dark') {{
            document.body.classList.add('dark-mode');
        }}
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
        with open(ERROR_LOG, "a", encoding="utf-8") as f:
            f.write(f"{datetime.now()}: Błąd w generate_html_report: {e}\n{traceback.format_exc()}\n")
        logging.error(f"❌ Błąd w generate_html_report: {e}")

# Główna funkcja

def main():
    try:
        print("✅ Skrypt uruchomiony — zaczynam analizę...")
        
        # Próbujemy pobrać nowe logi z FTP jeśli konfiguracja jest dostępna
        if FTP_HOST and FTP_USER and FTP_PASS:
            try:
                print("🔄 Pobieranie logów z FTP...")
                download_logs(FTP_DIR)
                download_logs(FTP_DIR2)
                download_logs(FTP_DIR3)
                print("✅ Logi pobrane z FTP")
            except Exception as ftp_error:
                print(f"⚠️ Nie można pobrać logów z FTP: {ftp_error}")
                print("📂 Analizuję istniejące logi z log_cache...")
                logging.warning(f"⚠️ FTP niedostępne: {ftp_error} — używam lokalnych logów")
        else:
            print("📂 FTP nie skonfigurowany — analizuję istniejące logi z log_cache...")
            logging.info("📂 Analiza lokalnych logów z log_cache")
        
        events, event_counts = analyze_logs()
        errors, warnings, warning_types, mod_issues, sessions_df, admin_cmds = detect_errors_and_stats(events)
        df_saves, save_charts = handle_saves(events)
        warning_charts = monitor_and_predict(warnings)
        other_charts = generate_charts(pd.DataFrame(events), sessions_df, admin_cmds)
        df = export_data(events, sessions_df)
        mod_charts = export_mod_issues(df, mod_issues)
        other_charts.update(mod_charts)
        generate_html_report(events, event_counts, errors, warnings, warning_types, mod_issues, sessions_df, admin_cmds, save_charts, warning_charts, other_charts)
        print("✅ Raport HTML wygenerowany: docs/index.html")
        logging.info("✅ Analiza zakończona pomyślnie.")
    except Exception as e:
        with open(ERROR_LOG, "a", encoding="utf-8") as f:
            f.write(f"{datetime.now()}: Błąd w main: {e}\n{traceback.format_exc()}\n")
        print(f"❌ Błąd: {e}")
        logging.error(f"❌ Błąd w main: {e}")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print("❌ Błąd podczas działania skryptu:")
        traceback.print_exc()
        sys.exit(1)