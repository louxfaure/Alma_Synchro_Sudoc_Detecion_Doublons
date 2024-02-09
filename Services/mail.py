#!/usr/bin/python3
# -*- coding: utf-8 -*-
import os
import smtplib,ssl
import logging
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
from email.mime.text import MIMEText

__maillogin__ = os.getenv('MAIL_LOGIN_UB')
__mailpwd__ = os.getenv('MAIL_PWD_UB')
__smtpserver__ = os.getenv('SMTP_SERVER_UB')
__smtpport__ = os.getenv('SMTP_PORT_UB')


class Mail(object):

    def __init__(self, maillogin=__maillogin__, mailpwd=__mailpwd__,smtpserver=__smtpserver__,smtpport=__smtpport__):
        self.logger = logging.getLogger()
        if maillogin is None:
            raise Exception("Fournir un login ENT")
        if mailpwd is None:
            raise Exception("Fournir un mot de passe ENT")
        if smtpserver is None:
            raise Exception("Fournir un serveur SMTP")
        if smtpport is None:
            raise Exception("Fournir un port")
        self.maillogin = maillogin
        self.mailpwd = mailpwd
        self.smtpserver=smtpserver
        self.smtpport=smtpport
        
    def envoie(self, mailfrom, mailto, subject, text,fichiers=[]):
        msg = MIMEMultipart()
        msg['From'] = mailfrom
        msg['To'] = mailto
        msg['Subject'] = subject 
        msg.attach(MIMEText(text))
        # gestion ds pj
        if len(fichiers)>0 :
            for fichier in fichiers :
               nom_pj = os.path.basename(fichier)
               f = open(fichier, 'rb')
               pj = MIMEApplication(f.read(),_subtype="txt")
               f.close()
               pj.add_header('Content-Disposition', 'attachment', filename=nom_pj)
               msg.attach(pj)
        context = ssl.create_default_context()
        try :
            mailserver = smtplib.SMTP(self.smtpserver, self.smtpport)
            mailserver.ehlo()
            mailserver.starttls(context=context) # Secure the connection
            mailserver.ehlo()
            mailserver.login(self.maillogin, self.mailpwd)
            mailserver.sendmail(mailfrom, mailto, msg.as_string())
        except Exception as e:
            print(e)
        finally:
            mailserver.quit()
            return "Message envoyé"
        