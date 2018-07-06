'''
表迁移  spicespirit => stat
'''

import pymysql
import time
import math

readDB = {
    'host':'47.95.20.154',
    'user':'tangzhen',
    'password':'bnueh73KcFvEfdaQ',
    'database':'spicespirit_online',
    'port':3306
}

readKingshard= {
    'host':'47.95.20.154',
    'user':'kingshard',
    'password':'NDAzMjIzNGE3',
    'database':'spicespirit_online_shard',
    'port':9696
}

readKingshard_3306= {
    'host':'47.95.20.154',
    'user':'tangzhen',
    'password':'bnueh73KcFvEfdaQ',
    'database':'spicespirit_online_shard',
    'port':3306
}

writeDb= {
    'host':'47.95.20.154',
    'user':'tangzhen',
    'password':'bnueh73KcFvEfdaQ',
    'database':'stat',
    'port':3307
}

table_list = [
    'a_batch_send_activity',            #批量发优惠表
    'a_batch_send_activity_detail',     #批量发优惠详情表
    'a_rechargeable_card',              #卡批次信息表
    'a_rechargeable_card_attribute',    #卡属性详情
    'a_rechargeable_card_detail',       #卡号表
    'a_rechargeable_card_export_log',   #导出卡列表日志
    'a_rechargeable_card_log',          #充值卡使用日志表
    'a_voucher',                        #兑换券主表
    'a_voucher_detail',                 #兑换券详情表
    'g_discount_card',                  #折扣卡主表
    'g_goods',                          #商品表
    'g_goods_sku',                      #商品属性详情表
    'g_goods_toclear',                  #店面商品估清
    'g_package_goods',                  #套餐商品对映表
    'l_deliver',                        #站点调拨表
    'l_deliver_detail',
    'l_difference',                     #差异单表
    'l_difference_detail',              #差异单详情表
    'l_loss_detail',                    #报损明细表
    'l_sell_num',                       #店面销售商品总计表
    'l_station',                        #站点表
    'l_station_stock',                  #站点库存表
    'l_station_tag',                    #站点标签表
    'l_stock',                          #库存表
    'l_stock_statistics',               #库存统计表
    'l_try_eat',                        #试吃log表
    's_account',                        #内部用户表
    's_account_extend',                 #账号补充
    's_menu',                           #菜单表
    's_permission',                     #权限标
    's_system',                         #系统表
    'scm_area',                         #区域表
    'st_direct_sale_compared',          #直销关系对应
    'st_sell_card',                     #卖卡统计表
    'u_user_activity',                  #用户活动表
    'wc_pusher_hot_statistics',         #推广统计
    'wc_pusher_statistics',             #推广统计
    's_sales',
    's_sales_relation'
];

table_list_increment = [
    'l_collating',                      #盘点表
    'wc_wechat_pusher',                 #推广人员二维码表
    'u_address',                        #物流地址
    'u_user',                           #用户信息表
    'l_collating_cost',                 #盘点成本表
    'l_collating_detail',               #盘点结果表
    'l_stock_collating',                # 盘点结果表
    'l_stock_hour',                     # 站点商品每小时库存量
    'wc_wechat',                        #微信用户信息表   是否取关状态更新？
    'o_invoice_order',                    #发票
    'wc_wechat_relation_log',             #微信log
    'a_activity_log',
    'dl_delivery',
]

table_ks=[
    'a_activity_log',
    'dl_delivery',
]

PAGE_SIZE = 5000

read_conn = pymysql.connect(host=readDB['host'], port=readDB['port'], user=readDB['user'], passwd=readDB['password'], db=readDB['database'], charset='utf8')
cur_read = read_conn.cursor()

ks_conn = pymysql.connect(host=readKingshard['host'], port=readKingshard['port'], user=readKingshard['user'], passwd=readKingshard['password'], db=readKingshard['database'], charset='utf8')
cur_ks = ks_conn.cursor()

write_conn = pymysql.connect(host=writeDb['host'], port=writeDb['port'], user=writeDb['user'], passwd=writeDb['password'], db=writeDb['database'], charset='utf8')
cur_write = write_conn.cursor()


'''
全量同步表
'''
def syncAll():
    # 四个参数分别为数据库地址，用户名，密码，数据库名称
    for i in range(len(table_list)):
        table = table_list[i].lower()
        print('开始 ',table,'=======================================================================')
        cur_read.execute("SELECT COUNT(*) FROM information_schema.COLUMNS WHERE  table_schema='%s' AND table_name='%s'  " % (readDB['database'],table))
        table_col_count = cur_read.fetchone()

        # 需要迁移的数据库查询表的结构
        cur_read.execute('show create table  %s' % table)
        result = cur_read.fetchall()
        create_sql = result[0][1]

        # 查询需要迁移的数据库表的数据条数
        cur_read.execute('select count(*) from  %s ' %  table)
        total = cur_read.fetchone()
        page = total[0] / PAGE_SIZE
        page1 = total[0] % PAGE_SIZE
        if page1 != 0:
            page = page + 1

        # 目标数据库创建表
        cur_write.execute( "SELECT table_name FROM information_schema.`TABLES` WHERE table_schema='%s' AND  table_name = '%s'  " % (writeDb['database'],table) )
        table_name = cur_write.fetchone()

        if table_name is None:
            cur_write.execute(create_sql)
        else:
            cur_write.execute(" truncate table %s " %  str(table).lower() )


        for p in range(0, int(page) ):
            try:
                print('开始', table, '的第', p + 1, '页查询')
                if p == 0:
                    limit_param = ' limit ' + str(p * PAGE_SIZE) + ',' + str(PAGE_SIZE)
                else:
                    limit_param = ' limit ' + str(p * PAGE_SIZE) + ',' + str(PAGE_SIZE)
                sql = 'select * from  %s  %s ' % ( table , limit_param)

                cur_read.execute('select * from  %s  %s ' % ( table , limit_param) )
                inserts = cur_read.fetchall()
                param = ''
                for i in range(0, table_col_count[0]):
                    param = param + '%s,'

                print('开始插入')
                cur_write.executemany('replace into ' + table + ' values (' + param[0:-1] + ')', inserts)
                print(table, '的第', p + 1, '页, 插入完成, 还有', math.ceil(page - p - 1), '页')
                write_conn.commit()
                print(p,page)
                if p == page:
                    break
            except Exception as e:
                print(e)
                time.sleep(60)
                # cur_read = read_conn.cursor()
                # cur_write = write_conn.cursor()

        print(table, ' 插入完成======================================================================== ',"\n\n")

'''
增量同步表
'''
def syncIncrement():
    # 四个参数分别为数据库地址，用户名，密码，数据库名称
    for i in range(len(table_list_increment)):
        table = table_list_increment[i].lower()
        print('开始 ',table,'')

        # kindshard ID 会有重复
        if table not in table_ks:
            print(table, '----------spicespirit_online---------')
            cur_read.execute("SELECT column_name FROM information_schema.COLUMNS WHERE  table_schema='%s' AND table_name='%s'  " % (readDB['database'], table))
            table_cols = cur_read.fetchall()
        else:
            print(table, '----------spicespirit_kingshard---------')
            cur_ks.execute("SELECT column_name FROM information_schema.COLUMNS WHERE  table_schema='%s' AND table_name='%s'  " % (readKingshard['database'], table))
            table_cols = cur_ks.fetchall()
            table_cols = table_cols[1:]

        table_cols_list=[]
        for t in list(table_cols):
            if isinstance(t, tuple):
                table_cols_list.append(list(t)[0])
            else:
                pass

        # 需要迁移的数据库查询表的结构
        cur_read.execute('show create table  %s' % table)
        result = cur_read.fetchall()
        create_sql = result[0][1]

        # 目标数据库创建表
        cur_write.execute( "SELECT table_name FROM information_schema.`TABLES` WHERE table_schema='%s' AND  table_name = '%s'  " % (writeDb['database'],table) )
        table_name = cur_write.fetchone()
        if table_name is None:
            cur_write.execute(create_sql)

        # 查询需要迁移的数据库表的数据条数
        cur_write.execute("SELECT IFNULL(max(gmt_created),0) as log_maxtime FROM %s  " % table)
        increment_point = cur_write.fetchone()

        cur_read.execute('select count(*) from   %s  WHERE gmt_created > %s  ' % (table, increment_point[0]))
        total = cur_read.fetchone()

        page = total[0] / PAGE_SIZE
        page1 = total[0] % PAGE_SIZE
        if page1 != 0:
            page = page + 1

        for p in range(0, int(page) ):
            try:
                print('开始', table, '的第', p + 1, '页查询')
                if p == 0:
                    limit_param = ' limit ' + str(p * PAGE_SIZE) + ',' + str(PAGE_SIZE)
                else:
                    limit_param = ' limit ' + str(p * PAGE_SIZE) + ',' + str(PAGE_SIZE)
                sql = 'select * from  %s  %s ' % ( table , limit_param)

                cols_string = ','.join(table_cols_list)
                if table not in table_ks:
                    print(table,'----------spicespirit_online---------')
                    cur_read.execute('select * from  %s WHERE gmt_created > %s  %s   ' % ( table , increment_point[0] , limit_param) )
                    inserts = cur_read.fetchall()
                else:
                    print(table, '----------spicespirit_kingshard---------')
                    cur_ks.execute('select %s from  %s WHERE gmt_created > %s  %s   ' % (cols_string,table, increment_point[0], limit_param))
                    inserts = cur_ks.fetchall()

                param = ''
                for i in range(0, len(table_cols_list)):
                    param = param + '%s,'

                print('开始插入')

                cur_write.executemany('  insert into   ' + table + ' ( '+ cols_string +' ) values (' + param[0:-1] + ')' , inserts)
                print(table, '的第', p + 1, '页, 插入完成, 还有', math.ceil(page - p - 1), '页')
                write_conn.commit()

                if p == page:
                    break
            except Exception as e:
                print(e)
                time.sleep(60)
                # cur_read = read_conn.cursor()
                # cur_write = write_conn.cursor()

        print(table, ' 插入完成',"\n\n")


'''
中间表 r_user_order_relation 同步
'''
def sync_r_user_order_relation(table):
    cur_write.execute("SELECT IFNULL(max(order_gmt_created),0) AS max_order_gmt_created FROM %s   " % table )
    increment_point = cur_write.fetchone()

    sql = '''
    SELECT B.user_id,B.`code`,B.`station_category`,B.`gmt_created` as order_gmt_created, 0 as user_gmt_created,B.station_id from 
    (
        SELECT A.*  from 
        (
            select  `code`,user_id,station_category,gmt_created,station_id  from r_sales_costs_hotdata  where  order_status=3 AND user_category !=2 and gmt_created > {0} and user_id not in ( select user_id from r_user_order_relation )
        ) A  
        order by A.gmt_created asc 
    ) B
    group by B.user_id
    '''.format(increment_point[0])

    cur_write.execute(sql)
    inserts = cur_write.fetchall()

    param = ''
    cols=['user_id','code','station_category','order_gmt_created','user_gmt_created','station_id']
    cols_string = ','.join(cols)

    for i in range(0, len(cols)):
        param = param + '%s,'

    print('开始插入')
    cur_write.executemany(' insert into  ' + table + '  ( ' + cols_string + ' )   ' + ' values   (' + param[0:-1] + ')', inserts)
    write_conn.commit()
    print('插入完成',"\n\n")

'''
关闭连接
'''
def connClose():
    cur_read.close()
    read_conn.close()

    cur_ks.close()
    ks_conn.close()

    cur_write.close()
    write_conn.close()


if __name__ == '__main__':
    syncAll()           #全表同步
    syncIncrement()     #增量表同步
    sync_r_user_order_relation('r_user_order_relation')        #中间表 r_user_order_relation 同步
    connClose()





