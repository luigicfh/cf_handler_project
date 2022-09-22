from google.cloud import firestore
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import smtplib
import ssl


def get_doc(db: firestore.Client, collection: str, id: str) -> dict:

    return db.collection(collection).document(id).get().to_dict()


def create_doc(db: firestore.Client, collection: str, id: str, doc: dict):

    doc_ref = db.collection(collection).document(id)

    doc_ref.set(doc)

    return id


def update_doc(db: firestore.Client, collection: str, id: str, doc: dict, state_msg=None) -> dict:

    if state_msg:

        doc['state_msg'] = state_msg

    db.collection(collection).document(id).set(doc)

    return db.collection(collection).document(id).get().to_dict()


def send_email(sender: str, password: str, to: list, subject: str, body: str) -> None:

    message = MIMEMultipart("alternative")
    message['Subject'] = subject
    message['From'] = sender
    message['To'] = ",".join(to)

    part1 = MIMEText(body, "plain")
    part2 = MIMEText(body, "html")

    message.attach(part1)
    message.attach(part2)

    context = ssl.create_default_context()
    with smtplib.SMTP_SSL("smtp.gmail.com", 465, context=context) as server:
        server.login(sender, password)
        server.sendmail(
            sender, to, message.as_string()
        )
