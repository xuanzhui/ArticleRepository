#!/usr/bin/env python
# -*- coding: utf-8 -*-

#it is the last step to extract all the poem content with every link

__author__ = 'xuanzhui'

import urllib.request
import re
import psycopg2
import traceback

class ExtractPoemCont:

    def extractPoem(self, conturl):
        poem=None
        try:
            #set timeout 8 seconds in case process blocks within too much time
            #use replace instead of ignore as the second parameter of decode and use GB18030 instead of gbk to support more characters
            urlcont=urllib.request.urlopen(conturl, timeout=8).read().decode('GB18030','replace')

        except Exception:
            urlcont=None
            print('Exception!!!!')
            print(conturl)
            print(traceback.format_exc())

        if urlcont:
            #pattern=r'<font face="幼圆" style="font-size: 16pt" color="#FFFFBF">(.*?)</font>'
            pattern=r'<font face="幼圆" style="font-size: \d*pt" color="#FFFFBF">(.*?)</font>'
            patternobj=re.compile(pattern)
            poem=patternobj.search(urlcont)

            if poem:
                poem=poem.group(1)
                poem=poem.replace('&nbsp;','')
                poem=poem.replace('<br>','')
            else:
                poem=None
                print('retrieve content failed!!!')

        return poem

    def store2db(self, conn, curl, aid):
        cont=self.extractPoem(curl)

        if not cont:
            return

        cur = conn.cursor()
        cur.execute("update articles set content = %s where id = %s",
            (cont, aid))
        conn.commit()
        cur.close()

    def storePerPage(self, password, recordsPerPage, offset, showprocess=False):
        query="select id, additional_info1 from articles order by id limit %d offset %d" % (recordsPerPage, offset)

        #conn can't be passed to child process
        conn = psycopg2.connect(database="poems", user="poet", password=password)

        cur=conn.cursor()
        cur.execute(query)
        reclist = cur.fetchall()
        cur.close()

        if showprocess:
            count=0
            totalcnt=len(reclist)
            pagenum=offset//recordsPerPage

        for aid, link in reclist:
            self.store2db(conn, link, aid)

            if showprocess:
                count+=1

                if count%10 == 0:
                    print('child processor %d: %d of %d (%0.2f%%) completed' % (pagenum, count, totalcnt, count/totalcnt * 100))

        if showprocess:
            print('child processor %d is done' % pagenum)

        conn.close()


    def batch_store2db(self, password, recordsPerPage=5000, showprocess=False):

        import multiprocessing
        import math

        processorNum = multiprocessing.cpu_count()

        conn = psycopg2.connect(database="poems", user="poet", password=password)

        cur=conn.cursor()
        cur.execute("select count(1) from articles")
        totalNum = cur.fetchone()[0]
        cur.close()

        conn.close()

        totalpage = math.ceil(totalNum/recordsPerPage)

        pool = multiprocessing.Pool(processorNum)
        pool.starmap(self.storePerPage, [(password, recordsPerPage, recordsPerPage*pagenum, showprocess) for pagenum in range(totalpage)])
        pool.close()
        pool.join()

    #this is only for the ones whose contents are still not filled
    def wipeleft(self, password, showprocess=True):
        conn = psycopg2.connect(database="poems", user="poet", password=password)

        cur=conn.cursor()
        cur.execute("select id, additional_info1 from articles where content='NULL'")
        recds = cur.fetchall()
        cur.close()

        for aid, link in recds:
            if showprocess:
                print('dealing with --> ', link)
            self.store2db(conn, link, aid)

        conn.close()


if __name__=='__main__':
    import getpass

    password=getpass.getpass('input password for user poet:')

    print('processing...')

    #ExtractPoemCont().batch_store2db(password, showprocess=True)
    ExtractPoemCont().wipeleft(password)

    print('done!')