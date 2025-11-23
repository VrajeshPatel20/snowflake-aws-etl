import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from etl_factory.utils.base import BaseHook

class SMTPHook(BaseHook):
    def __init__(self, config_section="SMTP", **kwargs):
        """
        Initialize the SMTPHook with any necessary parameters.
        """
        super().__init__(**kwargs)
        self.smtp_server = self.get_config("server", section=config_section.lower())
        self.smtp_port = self.get_config("port", section=config_section.lower())
        self.username = self.get_config("username", section=config_section.lower())
        self.password = self.get_config("password", section=config_section.lower())
        self.use_tls = self.get_config("use_tls", section=config_section.lower()).lower()

    def send_email(self, subject, recipients, body, sender=None):
        msg = MIMEMultipart()
        msg["From"] = sender or self.username
        msg["To"] = ", ".join(recipients) if isinstance(recipients, list) else recipients
        msg["Subject"] = subject
        msg.attach(MIMEText(body, "plain"))

        with smtplib.SMTP(self.smtp_server, self.smtp_port) as server:
            if self.use_tls:
                server.starttls()
            server.login(self.username, self.password)
            server.sendmail(msg["From"], recipients, msg.as_string())

    def execute(self):
        pass