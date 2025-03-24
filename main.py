import mysql.connector
import configparser
import yaml
import schedule
import time
import logging
from logging.handlers import TimedRotatingFileHandler
import os
import signal

# config.ini 파일 읽기
config = configparser.ConfigParser()
config.read("config.ini")

# 설정값 가져오기
log_level = config.get("logger", "level", fallback="INFO")
log_format = "%(asctime)s - %(levelname)s - %(message)s"
log_dir = config.get("logger", "log_dir", fallback="logs")
log_file = config.get("logger", "file", fallback="app.log")
when = config.get("logger", "when", fallback="D")
interval = config.getint("logger", "interval", fallback=1)
backup_count = config.getint("logger", "backup_count", fallback=7)

# 로그 디렉토리 확인 후 생성
if not os.path.exists(log_dir):
    os.makedirs(log_dir)

log_path = os.path.join(log_dir, log_file)

# 로깅 설정
logger = logging.getLogger("installer_server")
logger.setLevel(getattr(logging, log_level, logging.INFO))

# 핸들러 설정
handler = TimedRotatingFileHandler(
    log_path, when=when, interval=interval, backupCount=backup_count
)
formatter = logging.Formatter(log_format)
handler.setFormatter(formatter)

# 핸들러 추가
logger.addHandler(handler)

# 종료 신호 핸들러
def handle_sigterm(signum, frame):
    logger.info("프로세스가 정상 종료되었습니다.")
    exit(0)

# 종료 신호 핸들러 등록
signal.signal(signal.SIGTERM, handle_sigterm)

# INI 파일에서 DB 및 스케줄 설정 읽기
def load_config(config_file):
    config = configparser.ConfigParser()
    try:
        config.read(config_file, encoding='utf-8')
        db_config = {
            'host': config['mysql']['host'],
            'port': int(config['mysql'].get('port', 3306)),
            'user': config['mysql']['user'],
            'password': config['mysql']['password'],
            'database': config['mysql']['database'],
            'charset': config['mysql']['charset'],
            'collation': config['mysql']['collation'],
        }
        yaml_file_path = config['output']['yaml_file_path']
        interval = int(config['schedule']['interval_minutes'])
        return db_config, yaml_file_path, interval
    except (configparser.Error, KeyError, ValueError) as config_err:
        logger.error(f"Config 파일 오류: {config_err}")
        exit(1)

# MySQL 연결 재시도 함수
def connect_with_retry(db_config, retries=3, delay=5):
    for attempt in range(retries):
        try:
            connection = mysql.connector.connect(**db_config)
            logger.info("데이터베이스 연결 성공")
            return connection
        except mysql.connector.InterfaceError as conn_err:
            logger.warning(f"데이터베이스 연결 시도 {attempt + 1} 실패: {conn_err}")
            time.sleep(delay)
    logger.error("데이터베이스 연결 실패")
    return None

# MySQL 데이터 조회 및 YAML 저장
def fetch_and_save_data():
    try:
        db_config, yaml_file_path, _ = load_config(config_file)
        connection = connect_with_retry(db_config)
        if not connection:
            logger.error("데이터베이스 연결에 실패하여 작업을 종료합니다.")
            return

        cursor = connection.cursor(dictionary=True)
        cursor.execute("SELECT ProjectCode, Name, ProductType, ApiToken, Platform, LicenseKey FROM ApmProject WHERE LicenseKey != ''")
        results = cursor.fetchall()

        # 결과를 YAML로 저장
        try:
            with open(yaml_file_path, "w", encoding="utf-8") as yaml_file:
                yaml.dump(results, yaml_file, allow_unicode=True, default_flow_style=False)
            logger.info(f"프로젝트 메타타데이터를 YAML 파일로 저장했습니다: {yaml_file_path}")
        except (OSError, yaml.YAMLError) as yaml_err:
            logger.error(f"YAML 파일 저장 오류: {yaml_err}")

    except mysql.connector.ProgrammingError as prog_err:
        logger.error(f"SQL 문법 오류: {prog_err}")
    except mysql.connector.InterfaceError as iface_err:
        logger.error(f"데이터베이스 연결 오류: {iface_err}")
    except mysql.connector.Error as err:
        logger.error(f"일반적인 DB 오류: {err}")
    except Exception as ex:
        logger.error(f"예상하지 못한 오류 발생: {ex}")
    finally:
        if 'cursor' in locals() and cursor:
            cursor.close()
        if 'connection' in locals() and connection.is_connected():
            connection.close()
            logger.info("데이터베이스 연결을 종료했습니다.")

# INI 파일 경로
config_file = "config.ini"

# 스케줄러 설정
if __name__ == "__main__":
    db_config, yaml_file_path, interval = load_config(config_file)
    
    fetch_and_save_data()

    # 주기적으로 실행 설정
    schedule.every(interval).minutes.do(fetch_and_save_data)

    logger.info(f"{interval}분 후에 재실행됩니다.")

    while True:
        try:
            schedule.run_pending()
        except Exception as schedule_err:
            logger.error(f"스케줄 오류: {schedule_err}")
        time.sleep(1)