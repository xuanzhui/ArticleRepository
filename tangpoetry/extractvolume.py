#!/usr/bin/env python
# -*- coding: utf-8 -*-

#it is the first step to extract all the volumes info from http://www16.zzu.edu.cn/qtss/zzjpoem1.dll/query

__author__ = 'xuanzhui'

import urllib.request
import urllib.response
import io

class ExtractVolume:
    def __init__(self):
        self.strurl='http://www16.zzu.edu.cn/qtss/zzjpoem1.dll/query'
        self.HREF_PRE=r'<span style="font-size: 11pt"><a href="'
        self.HREF_PRE_LEN=len(self.HREF_PRE)
        self.POET_PRE=r'<font color="#FFFFB0">'
        self.POET_PRE_LEN=len(self.POET_PRE)
        self.contstrio=io.StringIO('\n')


    def retrieveWebCont(self):
        urlrep=urllib.request.urlopen(self.strurl)

        urlcont=''

        try:
            urlcont=urlrep.read().decode('gbk')

        except UnicodeDecodeError as e:
            print('UnicodeDecodeError',e.reason)

        self.contstrio=io.StringIO(urlcont)

        return urlcont


    def parsePoetsNLink(self, line):
        start_pos=line.find(self.HREF_PRE)
        if start_pos != -1:
            end_pos=line.find(r'">', start_pos + self.HREF_PRE_LEN)
            if end_pos == -1:
                return None
            ahref=line[start_pos+self.HREF_PRE_LEN : end_pos]

            a_end_pos=line.find(r'</a>', end_pos)
            volume=line[end_pos+len(r'">'):a_end_pos]

            poet_s_pos=line.find(self.POET_PRE, a_end_pos)
            if poet_s_pos == -1:
                return None

            poet_end_pos=line.find(r'</font>', poet_s_pos)
            poet_name=line[poet_s_pos+self.POET_PRE_LEN:poet_end_pos].replace('\u3000','')

            return poet_name, volume, ahref
        else:
            return None


    def write2file(self, filename):
        self.retrieveWebCont()

        count=0
        with open(filename,'w') as volumefw:
            for line in self.contstrio:
                res=self.parsePoetsNLink(line)
                if not res:
                    count+=1
                    volumefw.write("%s\t%s\t%s%s"%(res[0], res[1], res[2], '\n'))

        print("total volumes : %i"%(count))


    def store2db(self, password):
        import psycopg2

        self.retrieveWebCont()

        insertsql="INSERT INTO artists (name, dynasty, additional_info1, additional_info2) VALUES (%s, '唐', %s, %s)"
        selectsql="select id, additional_info1, additional_info2 from artists where name=%s"
        updatesql="update artists set additional_info1=%s, additional_info2=%s where id=%s"
        conn = psycopg2.connect(database="poems", user="poet", password=password)
        cur = conn.cursor()

        for line in self.contstrio:
            res=self.parsePoetsNLink(line)

            if res and res[0]:

                cur.execute(selectsql,(res[0],))
                recd=cur.fetchone()
                if not recd:
                    cur.execute(insertsql,(res[0],res[1],res[2]))
                else:
                    additional_info1 = ((recd[1]+','+res[1]) if recd[1] else res[1])
                    additional_info2 = ((recd[2]+','+res[2]) if recd[2] else res[2])
                    cur.execute(updatesql, (additional_info1, additional_info2, recd[0]))

        conn.commit()

        cur.close()
        conn.close()


if __name__=="__main__":
    import getpass

    password=getpass.getpass('input password for user poet:')

    print('processing...')
    ev=ExtractVolume()
    #ev.retrieveWebCont()
    ev.store2db(password)
    print('done')