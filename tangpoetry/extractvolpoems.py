#!/usr/bin/env python
# -*- coding: utf-8 -*-

#it is the second step to extract all the poem links from each volume

__author__ = 'xuanzhui'

import urllib.request
import re
import io
import os
import psycopg2

class ExtractPoemLinks:
    def encodeurl(self, strurl):
        s_pos = strurl.find('qnam=')
        e_pos = strurl.find('&qs')
        chstr = strurl[s_pos + len('qnam='):e_pos]
        #print(chstr)
        chstren = urllib.request.quote(chstr.encode('gbk'))
        newstr = strurl.replace(chstr, chstren)
        return newstr

    def getTotalPageNum(self, strio):
        numstr = '-1'
        num = -1
        for line in strio:
            if line.find(r'<font color="#FFFF00">共') != -1:
                s_pos = line.find('分')
                e_pos = line.find('页', s_pos)
                numstr = line[s_pos + 1:e_pos]
                break

        try:
            num = int(numstr)
        except ValueError as e:
            num = -1
            print('ValueError: ', e)

        return num

    def extractPoems(self, strurl, debugflag=False):
        baseurl = self.encodeurl(strurl)
        urlrep = urllib.request.urlopen(baseurl)
        try:
            urlcont = urlrep.read().decode('gb18030', 'ignore')
        except UnicodeDecodeError:
            print('**************************')
            print("UnicodeDecodeError when dealing with url: %s" % baseurl)
            print('**************************')

        pattern = r'<span style="font-size: 11pt"><a href="(.*?)">(.*?)</a></span>.*?<p align="center"><font color="#FFFFB0">(.*?)</font>'
        patternobj = re.compile(pattern)
        poems = patternobj.findall(urlcont)

        totalNum = self.getTotalPageNum(io.StringIO(urlcont))
        if totalNum > 1:
            if debugflag:
                print('querying additional pages')

            for pageNum in range(1, totalNum):
                surl = baseurl + ('&pn=' + str(pageNum + 1))

                if debugflag:
                    print('querying ' + surl)

                try :
                    urlcont = urllib.request.urlopen(surl).read().decode('gb18030', 'ignore')
                except UnicodeDecodeError:
                    print('**************************')
                    print("UnicodeDecodeError when dealing with url: %s" % surl)
                    print('**************************')

                poems.extend(patternobj.findall(urlcont))

        return poems

    '''
    #bug 以 第117卷 为例 该卷包含多个诗人
    def writeToFile(self, poetname, poems):
        with open(poetname + '.txt', 'w') as fw:
            for (ah, pn) in poems:
                fw.write("%s\t%s%s" % (pn, ah, '\n'))
                print("%s\t%s" % (pn, ah))

    def batch2file(self):
        with open('volumes.txt', 'r') as fr:
            count = 0
            for line in fr:
                if count < 10:
                    items = line.split()
                    self.writeToFile(items[0], self.extractPoems(items[2]))

                count += 1
    '''

    def store2db(self, conn, urls, additional_info1, debugflag=False):
        poetcache = {}
        cur = conn.cursor()

        urls=urls.split(',')

        for url in urls:

            if debugflag:
                print('executing url -> ', url)

            poemlinks = self.extractPoems(url)
            for link, title, poet in poemlinks:

                if debugflag:
                    print('storing -> ', poet, title, link)

                poetid = poetcache.get(poet)
                if not poetid:

                    if debugflag:
                        print(poet, 'not in cache ')

                    cur.execute("select id from artists where name = %s", (poet,))
                    record = cur.fetchone()
                    if not record:

                        if debugflag:
                            print('can not find <%s> in db, inserting now' % poet)

                        cur.execute("insert into artists(name,dynasty,additional_info1) values (%s,%s,%s) returning id",
                            (poet, '唐', additional_info1))
                        poetid=cur.fetchone()[0]

                    else:

                        if debugflag:
                            print("find <%s> in db" % poet)

                        poetid=record[0]

                    if debugflag:
                        print('add <%s> into cache' % poet)

                    poetcache[poet] = poetid

                else:
                    if debugflag:
                        print('<%s> already in cache, no operation on db' % poet)

                if debugflag:
                    print('inserting article <%s>' % title)

                cur.execute("insert into articles(artist_id, title, content, article_type, additional_info1) values (%s, %s, %s, %s, %s)",
                    (poetid, title, 'NULL', '诗', link))

                conn.commit()

        cur.close()


    def storePerPage(self, password, recordsPerPage, offset, debugflag=False, showprocess=False):
        #order by is very important!!!
        query="select id, additional_info1, additional_info2 from artists where additional_info2 is not null order by id limit %d offset %d" % (recordsPerPage, offset)

        #conn can't be passed to child process
        conn = psycopg2.connect(database="poems", user="poet", password=password)

        cur=conn.cursor()
        cur.execute(query)
        reclist = cur.fetchall()
        cur.close()

        if debugflag:
            print(recordsPerPage, offset, '--->', reclist)

        if showprocess:
            count=0
            totalcnt=len(reclist)
            cpid=os.getpid()

        for aid, additional_info1, link in reclist:
            #cur.execute("update artists set additional_info3='Y' where id=%s", (aid,))
            #conn.commit()
            self.store2db(conn, link, additional_info1, debugflag)

            if showprocess:
                count+=1

                print('child processor %d: %d of %d (%0.2f%%) completed' % (cpid, count, totalcnt, count/totalcnt * 100))

        if showprocess:
            print('child processor %d is done' % cpid)

        #cur.close()
        conn.close()


    def batch_store2db(self, password, debugflag=False, showprocess=False):

        import multiprocessing
        import math

        processorNum = multiprocessing.cpu_count()

        conn = psycopg2.connect(database="poems", user="poet", password=password)

        cur=conn.cursor()
        cur.execute("select count(1) from artists where additional_info2 is not null")
        totalNum = cur.fetchone()[0]
        cur.close()

        conn.close()

        recordsPerPage = math.ceil(totalNum/processorNum)

        pool = multiprocessing.Pool(processorNum)
        pool.starmap(self.storePerPage, [(password, recordsPerPage, recordsPerPage*pagenum, debugflag, showprocess) for pagenum in range(processorNum)])
        pool.close()
        pool.join()


if __name__=='__main__':
    import getpass

    password=getpass.getpass('input password for user poet:')

    print('processing...')

    ExtractPoemLinks().batch_store2db(password, showprocess=True)

    print('done!')