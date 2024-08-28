import logging
import os
import smtplib
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from email.mime.text import MIMEText

import paramiko

SFTP_HOSTNAME = os.environ["SFTP_HOSTNAME"]
INV_PATH = os.environ["INV_PATH"]
INV_USERNAME = os.environ["INV_USERNAME"]
INV_PASSWORD = os.environ["SFTP_PASS_INV"]
SSIM_PATH = os.environ["SSIM_PATH"]
SSIM_USERNAME = os.environ["SSIM_USERNAME"]
SSIM_PASSWORD = os.environ["SFTP_PASS_SSIM"]

SMTP_SERVER = os.environ["SMTP_SERVER"]
SENDER_EMAIL = os.environ["SENDER_EMAIL"]
RECEIVER_EMAIL = os.environ["RECEIVER_EMAIL"]

logging.basicConfig(
    stream=sys.stdout,
    level=logging.INFO,
    format="%(asctime)s - %(threadName)s - %(levelname)s - %(message)s",
)

connections = [
    {
        "hostname": SFTP_HOSTNAME,
        "username": INV_USERNAME,
        "password": INV_PASSWORD,
        "remote_path": INV_PATH,
    },
    {
        "hostname": SFTP_HOSTNAME,
        "username": SSIM_USERNAME,
        "password": SSIM_PASSWORD,
        "remote_path": SSIM_PATH,
    },
]

email_config = {
    "smtp_server": SMTP_SERVER,
    "sender_email": SENDER_EMAIL,
    "receiver_email": RECEIVER_EMAIL,
}


def sftp_cleanup(hostname, username, password, remote_path):
    try:
        transport = paramiko.Transport((hostname, 22))
        transport.connect(username=username, password=password)
        sftp = paramiko.SFTPClient.from_transport(transport)

        files = sftp.listdir(remote_path)
        logging.info(f"путь: {remote_path}, список файлов: {files}")

        if len(files) == 0:
            return {
                "hostname": hostname,
                "remote_path": remote_path,
                "result": "no_files",
            }

        md5sum_files = [f for f in files if f.endswith(".md5sum")]
        if len(md5sum_files) == 1 and len(files) == 1:
            sftp.remove(f"{remote_path}/{md5sum_files[0]}")
            logging.info(f"путь: {remote_path}, файл {md5sum_files[0]} был удален.")

            return {
                "hostname": hostname,
                "remote_path": remote_path,
                "result": "md5sum_deleted",
            }

        return {
            "hostname": hostname,
            "remote_path": remote_path,
            "result": "files_present",
            "files": files,
        }

    except Exception as e:
        logging.error(f"путь: {remote_path}, ошибка при подключении к SFTP: {e}")
        return {
            "hostname": hostname,
            "remote_path": remote_path,
            "result": "error",
            "error": str(e),
        }


def send_email(subject, body, email_config):
    msg = MIMEText(body, "html")
    msg["Subject"] = subject
    msg["From"] = email_config["sender_email"]
    msg["To"] = email_config["receiver_email"]
    smtp = smtplib.SMTP(email_config["smtp_server"])
    smtp.send_message(msg)
    smtp.quit()


def main():
    with ThreadPoolExecutor(max_workers=len(connections)) as executor:
        futures = [executor.submit(sftp_cleanup, **conn) for conn in connections]

        results = []
        for future in as_completed(futures):
            result = future.result()
            results.append(result)

    errors = []
    for result in results:
        if result["result"] == "files_present":
            errors.append(
                f"На сервере: <b>{result['hostname']}</b> по пути: <b>{result['remote_path']}"
                f"</b> остались необработанные файлы: <b>{', '.join(result['files'])}</b>"
            )
        elif result["result"] == "error":
            errors.append(
                f"Server {result['hostname']} вернул ошибку: {result['error']}"
            )

    if errors:
        error_message = "<br>&nbsp;<br>".join(errors)
        send_email("Critical Alert: SFTP Cleanup Errors", error_message, email_config)
        raise Exception("SFTP cleanup завершился с ошибкой.")
    else:
        logging.info("SFTP cleanup успешно завершена.")


if __name__ == "__main__":
    main()
