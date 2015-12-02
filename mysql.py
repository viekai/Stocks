#!/usr/bin/env python
# -*- coding: utf-8-*-
import cmd
import sys
import MySQLdb
import time

class DataBase(object):
    def __init__(self):
        try:
            self._conn=MySQLdb.connect(host='localhost',user='root',passwd='hellovi',port=3306)
            self._cur = self._conn.cursor()
            self.init_database()
        except MySQLdb.Error,e:
                 print "Mysql Error %d: %s" % (e.args[0], e.args[1])

    def execute(self, sql):
         self._cur.execute(sql)

    def check_init(self, database_name):
        sql = "show databases"
        self.execute(sql);
        ret = self._cur.fetchall()
        for line in ret:
            if database_name in line: 
                return True

        return False 
    #1. 以当前的年份为名字创建数据库
    #2. 创建用户表 :
    #     AccountName, 
    #     InitialCapital, 
    def init_database(self, reinit=False):
        try:
            ##
            year = time.strftime('%Y',time.localtime(time.time())) 
            db_name = "account_" + str(year)
            if self.check_init(db_name):
                if not reinit:
                    print "Database have been initlized!!"
                    self._conn.select_db(db_name)
                    return
                else:
                    sql = "drop database " + db_name
                    self.execute(sql);

            sql = "create database " + db_name 
            self.execute(sql)
            ##
            self._conn.select_db(db_name)
            ## 
            sql = "create table Account(AccountName char(20), InitialCapital float, Portion int)"
            self.execute(sql)
            
            print "Database Initilize OK!"

        except MySQLdb.Error,e:
                 print "Mysql Error %d: %s" % (e.args[0], e.args[1])
    
    def add_account(self, user_name, initialCapital, portion) :
        sql = "insert into Account values(\'%s\', %s, %s)" % (user_name, initialCapital, portion)
        self.execute(sql)
        return;

    def exit(self):
        self._conn.commit()
        self._cur.close()
        self._conn.close()

    def write(self):
        self._conn.commit()
        
class CLI(cmd.Cmd):
    Table_Struct = ["Date DATE", "C_Capital float", "I_Capital float", "Portion int", "Change_Portion float", "C_GAIN float", "T_GAIN float"] 
    Table_index = {"Date":0, "C_Capital":1, "I_Capital":2, "Portion":3, "Change_Portion":4, "C_GAIN":5, "T_GAIN":6} 

    Total_Struct = ["Date DATE", "C_Capital float", "I_Capital float", "Portion int", "Change_Portion int", "C_GAIN float", "T_GAIN float", "C_PRECENT float", "T_PRECENT float", "NetWorth float"]
    
    def __init__(self, db):
        cmd.Cmd.__init__(self)
        self.prompt = "> "    # define command prompt
        self._db = db

    def do_hello(self, arg):    # 定义hello命令所执行的操作
        print "hello again", arg, "!"
   
    def help_hello(self):   # 定义hello命令的帮助输出
        print "syntax: hello [message]",
        print "-- prints a hello message"
   
    def do_quit(self, arg): # 定义quit命令所执行的操作
        sys.exit(1)
   
    def help_quit(self):    # 定义quit命令的帮助输出
        print "syntax: quit",
        print "-- terminates the application"

    def help_init(self):
        print "Function:"
        print "----初始化数据库，创建Account表，但是此时Account表为空，需要调用addAccount添加用户"
        print "----随后调用build函数，对Account表中的所有用户创建记录表"
        print "Usage:"
        print "----init"
        
    def do_init(self, arg):
        self._db.init_database(); 
        self.do_build(arg)

    def do_reinit(self, arg):
        self._db.init_database(True); 
        self.do_build(arg)

    def do_exit(self, arg):
        print "Good bye!!"
        self._db.exit()
        sys.exit()

    def do_write(self, arg):
        self._db.write()

    def do_addAccount(self, arg):
        self._cmds = arg.split()
        if len(self._cmds) != 3:
            self.help_addAccount()
            return
        userName = str(self._cmds[0])
        initialCapital = float(self._cmds[1])
        portion = int(self._cmds[2])

        self._db.add_account(userName, initialCapital, portion);
        return;

    def help_addAccount(self):
        print "Function:"
        print "--------在Account数据表中添加用户，用户包含用户名, 初始资金量, 初始份额"
        print "Usage:"
        print "--------addAccount 账户名 资金量 初始份额"

    def do_showAccount(self, arg):
        print arg
        first = True 
        for account_info in self.get_account():
            if first:
                print "Account".rjust(8), "Capital".rjust(8), "Portion".rjust(8)
                first = False
            print str(account_info[0]).rjust(8), str(account_info[1]).rjust(8), str(account_info[2]).rjust(8)

    def get_account(self):
        sql = "select * from Account"
        self._db.execute(sql);
        account = self._db._cur.fetchall()
        for line in account:
            account_name = line[0]
            initilize_capital = float(line[1])
            portion = int(line[2])
            yield account_name, initilize_capital, portion

    def do_build(self, arg):
        have_init = False
        for account_info in self.get_account():
            print account_info
            have_init = True
            sql = "create table if not exists %s (%s)" % (account_info[0], ",".join(self.Table_Struct))
            self._db.execute(sql)
            sql = "insert into %s values(\'1989-10-10\', %s, %s, %s, 0, 0, 0)" % (account_info[0], account_info[1], account_info[1], account_info[2])
            print sql
            self._db.execute(sql)

        if not have_init:
            print "WARN: please add accrount and run build again"
            return;

        sql = "create table if not exists Total (%s)" % (",".join(self.Total_Struct))
        self._db.execute(sql)
        
        self.do_updateTotal(None)

    def help_build(self):
        print "Function:"
        print "---------为Account表中的每个user创建数据表, 表的格式为： 日期  当天资金 投入资金 当天份额数 转入/转出 当天盈利 总盈利"
        print "---------创建一张Total表, 表的格式为： 日期 当天资金 投入资金 当天份额数 申购/赎回 当天盈利 总盈利 当天盈利比率 总盈利比率 净值"
        print "---------该表有一个初始记录，除了日期以外所有项为0"
        print "Usage:"
        print "---------build"

    def help_updateAll(self):
        print "Function:"
        print "---------在表的格式为： 账户名字  日期  当天资金 当天份额数 转入/转出"
        print "Usage:"
        print "---------updateAll 账户名字 日期 当天资金 当天份额 转入/转出"

    def do_updateTotal(self, arg):
        accountName = []
        for account in self.get_account():
            accountName.append(account[0])

        sql = ""
        for account in accountName:
            if sql != "":
                sql += " union "
            sql += "select Date from %s " % (account)

        self._db.execute(sql);
        record = self._db._cur.fetchall()

        for date in record:
            date = date[0]
            C_Capital = 0
            I_Capital = 0
            Portion = 0
            Change_Portion = 0
            C_GAIN = 0 
            T_GAIN = 0
            C_PRECENT = 0
            T_PRECENT = 0
            NetWorth = 1;

            for account in accountName:
                sql = "select * from %s where Date = \'%s\'" % (account, date)
                self._db.execute(sql)
                info = self._db._cur.fetchone()
                if info == None:
                    continue
                C_Capital += float(info[self.Table_index["C_Capital"]])
                I_Capital += float(info[self.Table_index["I_Capital"]])
                Portion += int(info[self.Table_index["Portion"]])
                Change_Portion += int(info[self.Table_index["Change_Portion"]])
                C_GAIN += float(info[self.Table_index["C_GAIN"]])
                T_GAIN += float(info[self.Table_index["T_GAIN"]])

            prev_record = self.get_prev("Total", date)                

            change_capital = 0
            if prev_record == None:
                C_PRECENT = 0
            else:
                change_capital = prev_record[9] * Change_Portion
                prev_capital = prev_record[1]
                C_PRECENT = (C_Capital - prev_capital - change_capital) / prev_capital 

            T_PRECENT = (C_Capital - I_Capital) / I_Capital
            NetWorth = C_Capital / Portion 

            sql = "delete from Total where date = \'%s\'" % (date)
            self._db.execute(sql)
            sql = "insert into Total values(\'%s\', %s, %s, %s, %s, %s, %s, %s, %s, %s)" % (date, C_Capital, I_Capital, Portion, Change_Portion, C_GAIN, T_GAIN, C_PRECENT, T_PRECENT, NetWorth)
            self._db.execute(sql)
             
    def do_updateAll(self, arg):
        self._cmds = arg.split()
        if len(self._cmds) != 6:
            self.help_updateAll()
            return;

        account_name = self._cmds[0]
        date = self._cmds[1]
        #date = time.strftime('%Y-%m-%d',time.localtime(time.time())) 
        capital = float(self._cmds[2])
        i_capital = float(self._cmds[3])
        portion = int(self._cmds[4])
        change_portion = int(self._cmds[5])
        change_capital = 0
        c_gain = 0.0
        t_gain = 0.0

        sql = "delete from %s where Date = \'%s\'" % (account_name, date)
        self._db.execute(sql)

        record = self.get_prev(account_name, date)
        if change_portion != 0:
            prev_total = self.get_prev("Total", date)
            change_capital = change_portion * prev_total[9];
            i_capital = i_capital + change_capital 
            portion = portion + change_portion

        prev_capital = record[self.Table_index["C_Capital"]]
        c_gain = (capital - change_capital - prev_capital) 
        t_gain = (capital - i_capital)

        sql = "insert into %s values(\'%s\', %s, %s, %s, %s, %s, %s)" % (account_name, date, capital, i_capital, portion, change_capital, c_gain, t_gain)
        self._db.execute(sql)

        self.do_updateTotal(None)

    def get_prev(self, accountName, date):
        sql = "select * from %s where Date = (select max(Date) from HT where Date < \'%s\')" % (accountName, date)
        self._db.execute(sql)

        record = self._db._cur.fetchone();
        return record
        
    def help_update(self):
        print "Function:"
        print "----更新今天的记录"
        print "Usage:"
        print "----update Account_Name C_Capital Change_Portion"
        
    def do_update(self, arg):
        self._cmds = arg.split()
        if len(self._cmds) != 3:
            return;

        account_name = self._cmds[0]
        date = time.strftime('%Y-%m-%d',time.localtime(time.time())) 
        record = self.get_prev(account_name, date)

        C_Capital = float(self._cmds[1])
        Change_Portion = int(self._cmds[2])
        I_Capital = record[self.Table_index["I_Capital"]]
        Portion = record[self.Table_index["Portion"]]


        arg = [account_name, date, str(C_Capital), str(I_Capital), str(Portion), str(Change_Portion)]

        arg = " ".join(arg)

        self.do_updateAll(arg)

    def do_showTotal(self, arg):
        sql = "select * from account_2015.Total"
        self._db.execute(sql)

        self._db.execute(sql)

        report = self._db._cur.fetchall()

        for line in report:
            print line
        
    # 定义quit的快捷方式
    do_q = do_quit

if __name__ == '__main__':
    db = DataBase(); 
    cli = CLI(db)
    cli.cmdloop()
