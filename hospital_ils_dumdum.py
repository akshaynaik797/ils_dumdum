import base64
import json
import logging
import os
import time
from datetime import datetime
from datetime import timedelta
from random import randint

import msal
import pytz
import requests
import pdfkit
import mysql.connector
# Optional logging
# logging.basicConfig(level=logging.DEBUG)
from make_log import log_exceptions

email = 'mediclaim.ils.dumdum@gptgroup.co.in'
# email = 'ilsmediclaim@gptgroup.co.in'
# table_name = 'hospital_ils_mails'
table_name = 'hospital_ils_dumdum_mails'
if not os.path.exists('new_attach'):
    os.mkdir('new_attach')
if not os.path.exists('logs'):
    os.mkdir('logs')

conn_data = {'host': "iclaimdev.caq5osti8c47.ap-south-1.rds.amazonaws.com",
             'user': "admin",
             'password': "Welcome1!",
             'database': 'python'}

pdfconfig = pdfkit.configuration(wkhtmltopdf='/usr/bin/wkhtmltopdf')


def file_no(len):
    return str(randint((10 ** (len - 1)), 10 ** len)) + '_'


def file_blacklist(filename):
    fp = filename
    filename, file_extension = os.path.splitext(fp)
    ext = ['.pdf', '.htm', '.html']
    if file_extension not in ext:
        return False
    if fp.find('ATT00001') != -1:
        return False
    if (fp.find('MDI') != -1) and (fp.find('Query') == -1):
        return False
    if (fp.find('knee') != -1):
        return False
    if (fp.find('KYC') != -1):
        return False
    if fp.find('image') != -1:
        return False
    if (fp.find('DECLARATION') != -1):
        return False
    if (fp.find('Declaration') != -1):
        return False
    if (fp.find('notification') != -1):
        return False
    if (fp.find('CLAIMGENIEPOSTER') != -1):
        return False
    if (fp.find('declar') != -1):
        return False
    if (fp.find('PAYMENT_DETAIL') != -1):
        return False
    return True


config = json.load(open("gpt_parameters.json"))

# Create a preferably long-lived app instance which maintains a token cache.
app = msal.ConfidentialClientApplication(
    config["client_id"], authority=config["authority"],
    client_credential=config["secret"],
    # token_cache=...  # Default cache is in memory only.
    # You can learn how to use SerializableTokenCache from
    # https://msal-python.rtfd.io/en/latest/#msal.SerializableTokenCache
)

# The pattern to acquire a token looks like this.
result = None

# Firstly, looks up a token from cache
# Since we are looking for token for the current app, NOT for an end user,
# notice we give account parameter as None.
result = app.acquire_token_silent(config["scope"], account=None)

if not result:
    logging.info("No suitable token exists in cache. Let's get a new one from AAD.")
    result = app.acquire_token_for_client(scopes=config["scope"])


def get_mails():
    from_time, to_time = datetime.now() - timedelta(minutes=1), datetime.now()
    from_time = from_time.astimezone(pytz.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    to_time = to_time.astimezone(pytz.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    flag = 0
    while 1:
        try:
            if flag == 0:
                from_, to_ = from_time, to_time
                # print(from_time, to_time)
            print(from_, to_, datetime.now().astimezone(pytz.utc).strftime("%Y-%m-%dT%H:%M:%SZ"), datetime.now(), sep='||')
            flag = 1
            ##all code here
            if "access_token" in result:
                flag = 0
                while 1:
                    if flag == 0:
                        query = f"https://graph.microsoft.com/v1.0/users/{email}" \
                                f"/mailFolders/inbox/messages?$filter=(receivedDateTime ge {from_}) " \
                                f"and (receivedDateTime le {to_})"
                    flag = 1
                    graph_data2 = requests.get(query,
                                               headers={'Authorization': 'Bearer ' + result['access_token']}, ).json()
                    if 'value' in graph_data2:
                        for i in graph_data2['value']:
                            try:
                                date, subject, attach_path, sender = '', '', '', ''
                                format = "%Y-%m-%dT%H:%M:%SZ"
                                b = datetime.strptime(i['receivedDateTime'], format).replace(tzinfo=pytz.utc).astimezone(
                                    pytz.timezone('Asia/Kolkata')).replace(
                                    tzinfo=None)
                                date, subject, sender = b, i['subject'], i['sender']['emailAddress']['address']
                                # print(i['receivedDateTime'], b, i['subject'])
                                # print(i['sender']['emailAddress']['address'])
                                if i['hasAttachments'] is True:
                                    q = f"https://graph.microsoft.com/v1.0/users/{email}/mailFolders/inbox/messages/{i['id']}/attachments"
                                    attach_data = requests.get(q,
                                                               headers={'Authorization': 'Bearer ' + result[
                                                                   'access_token']}, ).json()
                                    for j in attach_data['value']:
                                        if '@odata.mediaContentType' in j:
                                            # print(j['@odata.mediaContentType'], j['name'])
                                            if file_blacklist(j['name']):
                                                j['name'] = file_no(4) + j['name']
                                                with open(os.path.join('new_attach', j['name']), 'w+b') as fp:
                                                    fp.write(base64.b64decode(j['contentBytes']))
                                                    # print('wrote', j['name'])
                                                    attach_path = j['name']
                                                attach_path = os.path.join('new_attach', attach_path)
                                else:
                                    filename = 'new_attach/' + file_no(8) + '.pdf'
                                    if i['body']['contentType'] == 'html':
                                        with open('new_attach/' + 'temp.html', 'w') as fp:
                                            fp.write(i['body']['content'])
                                        pdfkit.from_file('new_attach/temp.html', filename, configuration=pdfconfig)
                                        attach_path = filename
                                    elif i['body']['contentType'] == 'text':
                                        with open('new_attach/' + 'temp.text', 'w') as fp:
                                            fp.write(i['body']['content'])
                                        pdfkit.from_file('new_attach/temp.text', filename, configuration=pdfconfig)
                                        attach_path = filename
                                print(date, subject, attach_path, sender, sep='|')
                                with mysql.connector.connect(**conn_data) as con:
                                    cur = con.cursor()
                                    q = f"insert into {table_name} (`id`,`subject`,`date`,`sys_time`,`attach_path`,`completed`, `sender`) VALUES (%s, %s, %s, %s, %s, %s, %s)"
                                    data = (
                                    i['id'], subject, date, str(datetime.now()), os.path.abspath(attach_path), '', sender)
                                    cur.execute(q, data)
                                    con.commit()
                            except:
                                log_exceptions(mid=i['id'])
                                z = 1
                    else:
                        with open('logs/query.log', 'a') as fp:
                            print(query, file=fp)
                    if '@odata.nextLink' in graph_data2:
                        query = graph_data2['@odata.nextLink']
                    else:
                        break
            ##
            time.sleep(30)
            now_time = datetime.now().astimezone(pytz.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
            # print(to_time, now_time)
            from_ = to_
            to_ = now_time
        except:
            log_exceptions()


if __name__ == '__main__':
    while 1:
        try:
            get_mails()
        except:
            log_exceptions()
        print('error, see logs')
        time.sleep(60)
