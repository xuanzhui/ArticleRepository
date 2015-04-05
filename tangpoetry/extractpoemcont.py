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
        poem='NULL'
        try:

            urlcont=urllib.request.urlopen(conturl).read().decode('gbk', 'ignore')
            pattern=r'<font face="幼圆" style="font-size: 16pt" color="#FFFFBF">(.*?)</font>'
            patternobj=re.compile(pattern)
            poems=patternobj.findall(urlcont)

            poem=poems[0].replace('&nbsp;','')
            poem=poem.replace('<br>','')
        except Exception:
            print('Exception!!!!')
            print(conturl)
            print(traceback.format_exc())

        return poem

    def store2db(self, conn, curl, aid):
        cont=self.extractPoem(curl)
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


if __name__=='__main__':
    import getpass

    password=getpass.getpass('input password for user poet:')

    print('processing...')

    ExtractPoemCont().batch_store2db(password, showprocess=True)

    print('done!')