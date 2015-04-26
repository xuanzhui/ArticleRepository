#!/usr/bin/env python
# -*- coding: utf-8 -*-

#this is to add comment for poems

__author__ = 'xuanzhui'

import psycopg2

class ParseComment:

    def __init__(self):
        self.srctxt='唐诗鉴赏大辞典'

    def matchdbrecord(self, conn, poet, title):
        cur=conn.cursor()
        sqlstr = "select content from artists ats, articles atc where ats.name = %s and ats.id = atc.artist_id and atc.title = %s"
        cur.execute(sqlstr, (poet, title))

        res = cur.fetchall()

        cur.close()

        return res


    def operate(self):

        conn = psycopg2.connect(database="poems", user="poet", password='521uknow')

        with open(self.srctxt) as fr:
            count=0
            nomatch=[]
            artmark=0
            artcount=0

            title=''
            poet=''
            content=''
            comment=''

            for line in fr:

                if not line.startswith('\u3000') and not line.startswith('\t')\
                        and not line.startswith(' ') and len(line.strip()) != 0:
                    idx = line.find('|')
                    if idx == -1:
                        title = line.strip()

                        tidx = title.find('（')
                        if tidx != -1:
                            title = title[:tidx]

                        artmark=True
                        artcount=0
                    else:
                        poet=line[:idx]
                        title=line[idx+1:].strip()
                        tidx = title.find('（')
                        if tidx != -1:
                            title = title[:tidx]

                        res = self.matchdbrecord(conn, poet, title)

                        if len(res):
                            print(title, '-->', poet, '-->', res)
                        else:
                            nomatch.append((poet, title))

                        count+=1

                if artmark and len(line.strip()) != 0:
                    artcount+=1

                    if artcount == 3:
                        poet=line.strip()

                        artmark=False

                        res = self.matchdbrecord(conn, poet, title)

                        if len(res):
                            print(title, '-->', poet, '-->', res)
                        else:
                            nomatch.append((poet, title))

                        count+=1

                '''
                else:
                    tmp=line.split('，')
                    if len(tmp) == 2 and tmp[1] == tmp[2][:-1]:
                        pass
                '''
        print('total count ', count)
        print('total no match count ', len(nomatch))

        for a, b in nomatch:
            print(a, '--', b)

if __name__=='__main__':
    ParseComment().operate()