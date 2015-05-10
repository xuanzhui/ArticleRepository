#!/usr/bin/env python
# -*- coding: utf-8 -*-

# this is to add comment for poems

__author__ = 'xuanzhui'

import psycopg2
import re


class ParseComment:
    def __init__(self):
        self.srctxt = '唐诗鉴赏大辞典'
        # self.srctxt = 'nomatch.txt'
        self.conn = psycopg2.connect(database="poems", user="poet", password='')

    # douMatch means title and content should both match
    def matchdbrecord(self, poet, title, content='%'):
        cur = self.conn.cursor()

        sqlstr = "select atc.id, content from artists ats, articles atc where ats.name = %s " \
                         "and ats.id = atc.artist_id and atc.title like %s and content like %s"
        cur.execute(sqlstr, (poet, title, '%'+content+'%'))
        res = cur.fetchall()

        if not res:
            sqlstr = "select atc.id, content from artists ats, articles atc where ats.name = %s " \
                     "and ats.id = atc.artist_id and content like %s"
            tmp = content.split('，')
            for tm in tmp:
                cur.execute(sqlstr, (poet, '%'+tm+'%'))
                res = cur.fetchall()
                if res:
                    break

        cur.close()

        return res


    def parseCritics(self):
        pattern = re.compile('（(.*?)）')
        critics = set()

        with open(self.srctxt) as fr:
            for line in fr:
                mat = re.match(pattern, line.strip())
                if mat:
                    tmp = mat.group(1).split()
                    critics |= set(tmp)

        for s in critics:
            print(s)


    def addComment(self, article_id, comm_title, comm_content, comm_author):
        cur = self.conn.cursor()

        #check comment author
        check_comm_author = 'select id from artists where name = %s'
        cur.execute(check_comm_author, (comm_author,))
        res = cur.fetchone()

        if res:
            critic_id = res[0]
        else:
            insert_comm_author = 'insert into artists(name) values (%s) returning id'
            cur.execute(insert_comm_author, (comm_author,))
            res = cur.fetchone()
            if not res:
                print('error when inserting comment author')
            critic_id = res[0]

        #additional_info1 is a simple mark, for error operation
        insert_comm = 'insert into comments(article_id, critic_id, title, content, additional_info1) ' \
                      'values (%s,%s,%s,%s,%s)'
        cur.execute(insert_comm, (article_id, critic_id, comm_title, comm_content, 'M'))

        update_article_flag = "update articles set has_comment_flag='Y' where id = %s"
        cur.execute(update_article_flag, (article_id,))

        self.conn.commit()

        cur.close()


    def operate(self, debug = False):
        with open(self.srctxt) as fr:
            count = 0
            add_comm_count = 0
            nomatch = []
            manymat = []
            three_line_title = False
            entity_line = 0
            artcount = 0
            article_id = -1

            title = ''
            poet = ''
            comment = ''

            if debug:
                all_title = set()
                add_comm_title = set()

            for line in fr:
                if entity_line >= 1:
                    if entity_line == 1:
                        fid = line.find('|')
                        if fid != -1:
                            comment += (' '*fid + line[fid+1:] + '\n')
                            comment += (line[:fid] + '\n')
                        else:
                            comment += line
                    else:
                        comment += line

                if len(line.strip()) == 0:
                    continue

                # new comment entity
                if not line.startswith('\u3000') and not line.startswith('\t') \
                        and not line.startswith(' '):

                    article_id = -1
                    entity_line = 0
                    comment = ''

                    idx = line.find('|')
                    if idx == -1:
                        title = line.strip()

                        three_line_title = True
                        artcount = 0
                    else:
                        poet = line[:idx]
                        title = line[idx + 1:].strip()

                    tidx = title.find('（')
                    if tidx != -1:
                        title = title[:tidx]

                    count += 1

                    if debug:
                        debug_first_line = line[:-1]
                        all_title.add(line[:-1])

                if three_line_title:
                    artcount += 1

                    if artcount == 3:
                        three_line_title = False

                        poet = line.strip()

                entity_line += 1

                if (idx != -1 and entity_line == 3) or (idx == -1 and entity_line == 4):
                    #entity_line = 0

                    first_line = line.strip()[:-1]
                    #print('trying to match... ', title, '-->', poet, '-->', first_line)

                    res = self.matchdbrecord(poet, title, first_line)

                    if res:
                        if len(res) == 1:
                            article_id = res[0][0]
                            #print(title, '-->', poet, '-->', res)
                        else:
                            manymat.append((poet, title))
                    else:
                        nomatch.append((poet, title))

                #end of comment
                if line.strip().startswith('（') and line.strip().endswith('）'):
                    comm_author = line.strip()[1:-1]

                    if article_id != -1:
                        '''
                        print(poet)
                        print(title)
                        print(res[0][1])
                        print(comment)
                        print(comm_author)
                        '''
                        self.addComment(article_id, title+'-'+comm_author, comment, comm_author)
                        add_comm_count += 1

                        if debug:
                            add_comm_title.add(debug_first_line)

        self.conn.close()

        print('total count ', count)

        print('add comment count', add_comm_count)

        print('total no match count ', len(nomatch))
        for a, b in nomatch:
            print(a, '--', b)

        print('----------------')

        print('total too many match count ', len(manymat))

        for a, b in manymat:
            print(a, '--', b)

        if debug:
            print('-------- debug line diff lack comment --------')
            for tmp in all_title - add_comm_title:
                print(tmp)


if __name__ == '__main__':
    ParseComment().operate(True)