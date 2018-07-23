# -*- coding: utf-8 -*-
"""
Created on Fri Nov 17 09:45:27 2017
Edited on Thu Feb 22 21:02:00 2018
@author: jinanda
"""
###有一个细节问题没有解决，关于wind会修改数据，需要在数据库中加入时间戳列，对比时间戳来更新数据
from datetime import datetime
import pandas as pd
from sqlalchemy import create_engine
import pandas.io.sql as sql 

#构建基础表格
import table_func
import factor_db_raw

#------------------------------------------------------------------------------
#创建初始表格
#完全初始化表格，慎用！
def init_table_update():
#股票相关表---------------------------------------------------------------------
    #表stock_base_info
    table_func.create_table(table_name='stock_base_info',var_content="""stock_id char(10),wind_id char(10),
        stock_name char(40),corp_id char(10),ipo_date int(8),trading_end int(8)
        ,wind_l1 char(40),wind_l2 char(40),wind_l3 char(40)
        ,citi_l1 char(40),citi_l2 char(40),citi_l3 char(40)
        ,sw_l1 char(40),sw_l2 char(40),sw_l3 char(40)""")

################################################################################
#------------------------------------------------------------------------------    
#创建初始数据
#start_date='20071201'
#end_date  ='20180201'

def update_database(start_date='20070101',end_date=datetime.today().strftime('%Y%m%d')):
#日期表------------------------------------------------------------------------
    #数据trading_date
    table_func.write_table(table_name='date_trading',query="""
        select OB_OBJECT_ID ,f1_0012 as dates,f4_0012 as YN,f5_0012 as cause 
        from wind.TB_OBJECT_0012 
        where F1_0012 >=%s
        and F1_0012<=%s
        and f3_0012='CN'
        and f2_0012='SHFE'
        order by f1_0012
        """%(start_date,end_date))
#无风险利率---------------------------------------------------------------------
    #数据SHIBOR
    table_func.write_table(table_name='shibor',query="""
        select ob_object_id,f1_1821 as trading_date,f3_1821 as shibor_period,
        f4_1821/100 as rate,f6_1821 as shibor_id
        from wind.tb_object_1821
        where f1_1821>=%s
        and f1_1821<=%s"""%(start_date,end_date))
#股票相关表---------------------------------------------------------------------
    #数据stock_base_info
    table_func.write_table(table_name='stock_base_info',query="""
    select a.OB_OBJECT_ID,a.f1_0001 stock_id,a.f5_0001 stock_name,a.f16_0001 wind_id,a.f17_0001 corp_id
       ,b.f17_1090 ipo_date,b.f18_1090 trading_end
		,wind_ind.wind_l1,wind_ind.wind_l2,wind_ind.wind_l3
		,zz_ind.citi_l1,zz_ind.citi_l2,zz_ind.citi_l3
		,sw_ind.sw_l1,sw_ind.sw_l2,sw_ind.sw_l3
    from wind.TB_OBJECT_0001 a
    left join wind.TB_OBJECT_1090 b on b.f2_1090=a.f16_0001
	left join (
		Select F1_0001 stock_id
		,a1.name wind_l1,a2.name wind_l2,a3.name wind_l3
		From wind.tb_object_0001
			Inner Join wind.tb_object_1400 b On F1_1400 = f17_0001
			left  Join wind.tb_object_1022 a1 On (substr(f3_1400,1,4)=substr(a1.code,1,4) and a1.levelnum='2')
			left  Join wind.tb_object_1022 a2 On (substr(f3_1400,1,6)=substr(a2.code,1,6) and a2.levelnum='3')
			left  Join wind.tb_object_1022 a3 On (substr(f3_1400,1,8)=substr(a3.code,1,8) and a3.levelnum='4')
		Where F6_1400='1'
		And F12_0001='A'
		and b.f3_1400 like '62%') wind_ind on wind_ind.stock_id=a.f1_0001
	left join (
		Select F1_0001 stock_id
		,a1.name citi_l1,a2.name citi_l2,a3.name citi_l3
		From wind.tb_object_0001
			Inner Join wind.tb_object_1400 b On F1_1400 = f17_0001
			left Join wind.tb_object_1022 a1 On (substr(f3_1400,1,4)=substr(a1.code,1,4) and a1.levelnum='2')
			left Join wind.tb_object_1022 a2 On (substr(f3_1400,1,6)=substr(a2.code,1,6) and a2.levelnum='3')
			left Join wind.tb_object_1022 a3 On (substr(f3_1400,1,8)=substr(a3.code,1,8) and a3.levelnum='4')
		Where F6_1400='1'
		And F12_0001='A'
		and b.f3_1400 like 'b1%')	zz_ind on a.f1_0001=zz_ind.stock_id
	left join (
		Select F1_0001 stock_id
		,a1.name sw_l1,a2.name sw_l2,a3.name sw_l3
		From wind.tb_object_0001
    		Inner Join wind.tb_object_1476 b On F1_1476 = f16_0001
          Join wind.tb_object_1022 a1 On (substr(f2_1476,1,4)=substr(a1.code,1,4) and a1.levelnum='2')
          Join wind.tb_object_1022 a2 On (substr(f2_1476,1,6)=substr(a2.code,1,6) and a2.levelnum='3')
          Join wind.tb_object_1022 a3 On (substr(f2_1476,1,8)=substr(a3.code,1,8) and a3.levelnum='4')
		Where F5_1476='1'
		and a1.used='1'
		And F12_0001='A'
		and b.f2_1476 like '61%')	sw_ind on a.f1_0001=sw_ind.stock_id		
   where a.f12_0001='A'
   and a.f24_0001 is not null""")
    
    #数据stock_reprice
    table_func.write_table(table_name='stock_reprice',query="""
        select ob_object_id, b.f1_0001 as stock_id,a.f1_1425 as wind_id, a.f2_1425 as trading_date,
        a.f3_1425 as close_reprice_lastday,a.f4_1425 as open_reprice,a.f5_1425 as high_reprice,a.f6_1425 as low_reprice,
        a.f7_1425 as close_reprice,a.f8_1425 as raw_volume, a.f9_1425/10 as raw_amount, a.f10_1425 as reprice_factor
            from wind.tb_object_1425 a
            inner join
            (select f1_0001,f16_0001,f17_0001
            from wind.tb_object_0001
            where f12_0001='A'
            and f24_0001 is not null
            ) b on a.f1_1425=b.f16_0001
             where a.f2_1425>=%s
            and a.f2_1425<=%s
            """%(start_date,end_date))   
    #数据stock_return
    table_func.write_table(table_name='stock_return',query="""
        select ob_object_id,b.f1_0001 as stock_id,f1_5004 as wind_id,f2_5004 as trading_date,f5_5004 as avg_price,
        f6_5004/100 as turnover,f7_5004/100 as daily_return,f8_5004/100 as amplitude,f9_5004 as total_mv,
        f10_5004 as float_mv,f15_5004 as pb,f16_5004 as pe_ttm,f22_5004 as trading_state
        from wind.tb_object_5004 a
            inner join
            (select f1_0001,f16_0001,f17_0001
            from wind.tb_object_0001
            where f12_0001='A'
            and f24_0001 is not null
            ) b on a.f1_5004=b.f16_0001
            where a.f2_5004>=%s
            and a.f2_5004<=%s
            """%(start_date,end_date))
    #数据stock_weekly_return   
    table_func.write_table(table_name='stock_weekly_return',query="""
        select ob_object_id,b.f1_0001 as stock_id,f1_5005 as wind_id,f2_5005 as last_trading_date,f4_5005/100 as weekly_return,
        f5_5005/100 as total_turnover,f7_5005 as total_amount
        from wind.tb_object_5005 a
            inner join
            (select f1_0001,f16_0001,f17_0001
            from wind.tb_object_0001
            where f12_0001='A'
            and f24_0001 is not null
            ) b on a.f1_5005=b.f16_0001
            where a.f2_5005>=%s
            and a.f2_5005<=%s
            """%(start_date,end_date)) 
    #数据stock_monthly_return   
    table_func.write_table(table_name='stock_monthly_return',query="""
        select ob_object_id,b.f1_0001 as stock_id,f1_5006 as wind_id,f2_5006 as last_trading_date,f4_5006/100 as monthly_return,
        f5_5006/100 as total_turnover,f7_5006 as total_amount
        from wind.tb_object_5006 a
            inner join
            (select f1_0001,f16_0001,f17_0001
            from wind.tb_object_0001
            where f12_0001='A'
            and f24_0001 is not null
            ) b on a.f1_5006=b.f16_0001
            where a.f2_5006>=%s
            and a.f2_5006<=%s
            """%(start_date,end_date)) 
#财务报表-----------------------------------------------------------------------       
    #数据report_asset
    #根据2007年1月1日以后实施的新准则编制
    table_func.write_table(table_name='report_asset',query="""
        select ob_object_id,b.f1_0001 as stock_id,f1_1853 as corp_id,f2_1853 as report_period,
        NVL(F3_1853,TO_CHAR(a.RP_GEN_DATETIME,'YYYYMMDD')) as declare_date,
        f4_1853 as report_type,f9_1853,f10_1853,f11_1853,f12_1853,f13_1853,f14_1853,f15_1853,f16_1853,f17_1853,f18_1853,
        f19_1853,f20_1853,f21_1853,f22_1853,f23_1853,f25_1853,f26_1853,f27_1853,f28_1853,f29_1853,f30_1853,f31_1853,
        f32_1853,f33_1853,f34_1853,f35_1853,f36_1853,f37_1853,f38_1853,f39_1853,f40_1853,f41_1853,f42_1853,f43_1853,
        f44_1853,f46_1853,f74_1853,f75_1853,f76_1853,f77_1853,f78_1853,f79_1853,f80_1853,f81_1853,f82_1853,f83_1853,
        f84_1853,f85_1853,f86_1853,f87_1853,f88_1853,f89_1853,f90_1853,f91_1853,f93_1853,f94_1853,f95_1853,f96_1853,
        f97_1853,f98_1853,f99_1853,f100_1853,f101_1853,f103_1853,f128_1853,f129_1853,f130_1853,f131_1853,f132_1853,
        f133_1853,f134_1853,f135_1853,f136_1853,f137_1853,f138_1853,f140_1853,f141_1853,f142_1853,f143_1853,f145_1853,
        f146_1853,f147_1853,f148_1853,f156_1853,f159_1853,f162_1853,f164_1853,f165_1853
        from wind.tb_object_1853 a
            inner join
            (select f1_0001,f16_0001,f17_0001
            from wind.tb_object_0001
            where f12_0001='A'
            and f24_0001 is not null
            ) b on a.f1_1853=b.f17_0001
            where a.f3_1853>=%s
            and a.f3_1853<=%s
            and a.f6_1853='2'
            and a.f7_1853=1
            """%(start_date,end_date))
       
    #数据report_profit
    #根据2007年1月1日以后实施的新准则编制
    table_func.write_table(table_name='report_profit',query="""
        select ob_object_id,b.f1_0001 as stock_id,a.f1_1854 as corp_id,f2_1854 as report_period,
        NVL(F3_1854,TO_CHAR(a.RP_GEN_DATETIME,'YYYYMMDD')) as declare_date,
        f4_1854 as report_type,f9_1854,f10_1854,f11_1854,f12_1854,f13_1854,f14_1854,f15_1854,f16_1854,f17_1854,f48_1854,
        f49_1854,f50_1854,f51_1854,f55_1854,f56_1854,f60_1854,f61_1854,f62_1854,f80_1854,f81_1854,f82_1854,f83_1854,
        f84_1854,f85_1854,f86_1854,f87_1854,f88_1854,f89_1854,f92_1854,f93_1854,f94_1854,f95_1854,f96_1854
        from wind.tb_object_1854 a
            inner join
            (select f1_0001,f16_0001,f17_0001
            from wind.tb_object_0001
            where f12_0001='A'
            and f24_0001 is not null
            ) b on a.f1_1854=b.f17_0001
            where a.f3_1854>=%s
            and a.f3_1854<=%s
            and a.f6_1854='2'
            and a.f7_1854=1
            """%(start_date,end_date))
            
    #数据report_cashflow
    #根据2007年1月1日以后实施的新准则编制
    table_func.write_table(table_name='report_cashflow',query="""
        select ob_object_id,b.f1_0001 as stock_id,f1_1855 as corp_id,f2_1855 as report_period,
        NVL(F3_1855,TO_CHAR(a.RP_GEN_DATETIME,'YYYYMMDD')) as declare_date,
        f4_1855 as report_type,f9_1855,f10_1855,f11_1855,f25_1855,f26_1855,f27_1855,f28_1855,f29_1855,f37_1855,
        f48_1855,f49_1855,f50_1855,f53_1855,f57_1855,f68_1855,f75_1855,f82_1855,f83_1855,f84_1855,f85_1855
        from wind.tb_object_1855 a
            inner join
            (select f1_0001,f16_0001,f17_0001
            from wind.tb_object_0001
            where f12_0001='A'
            and f24_0001 is not null
            ) b on a.f1_1855=b.f17_0001
            where a.f3_1855>=%s
            and a.f3_1855<=%s
            and a.f6_1855='2'
            and a.f7_1855=1
            """%(start_date,end_date))
            
    #数据report_1087,业绩预告
    table_func.write_table(table_name='report_1087',query="""
        select ob_object_id,b.f1_0001 as stock_id,ob_object_name_1087,f6_1087,f8_1087,f9_1087,f10_1087,f11_1087,
        f12_1087,f13_1087,f14_1087,f15_1087,f16_1087,f17_1087
        from wind.tb_object_1087 a            
            inner join
            (select f1_0001,f16_0001,f17_0001
            from wind.tb_object_0001
            where f12_0001='A'
            and f24_0001 is not null
            ) b on a.f2_1087=b.f17_0001
            where a.f8_1087>=%s
            and a.f8_1087<=%s
            """%(start_date,end_date))

    #数据report_5034,财务衍生指标表
    table_func.write_table(table_name='report_5034',query="""
        select ob_object_id,b.f1_0001 as stock_id,f3_5034,f4_5034,f5_5034,f6_5034,f7_5034,f8_5034,f9_5034,f10_5034,f11_5034,
        f12_5034,f13_5034,f14_5034,f15_5034,f16_5034,f17_5034,f18_5034,f19_5034,f20_5034,f21_5034,f22_5034,f23_5034,f24_5034,
        f25_5034,f26_5034,f27_5034,f28_5034,f29_5034,f30_5034,f31_5034,f32_5034,f33_5034,f34_5034,f35_5034,f36_5034,f37_5034,
        f38_5034,f39_5034,f40_5034,f41_5034,f42_5034,f43_5034,f44_5034,f45_5034,f46_5034,f47_5034,f48_5034,f49_5034,f50_5034,
        f51_5034,f52_5034,f53_5034,f54_5034,f55_5034,f56_5034,f57_5034,f58_5034,f59_5034,f60_5034,f61_5034,f62_5034,f63_5034,
        f64_5034,f65_5034,f66_5034,f67_5034,f68_5034,f69_5034,f70_5034,f71_5034,f72_5034,f73_5034,f74_5034,f75_5034,f76_5034,
        f77_5034,f78_5034,f79_5034,f80_5034,f81_5034,f82_5034,f83_5034,f84_5034,f85_5034,f86_5034,f87_5034,f88_5034,f89_5034,
        f90_5034,f91_5034,f92_5034,f93_5034,f94_5034,f95_5034,f96_5034,f97_5034,f98_5034,f99_5034,f100_5034,f101_5034,f102_5034,
        f103_5034,f104_5034,f105_5034,f106_5034,f107_5034,f108_5034,f109_5034,f110_5034,f111_5034,f112_5034,f113_5034,f114_5034,
        f115_5034,f116_5034,f117_5034,f118_5034,f119_5034,f120_5034,f121_5034,f122_5034,f123_5034,f124_5034,f125_5034,f126_5034,
        f127_5034,f128_5034,f129_5034,f130_5034,f131_5034,f132_5034,f133_5034,f134_5034,f135_5034,f136_5034,f137_5034,f138_5034,
        f139_5034,f140_5034,f141_5034,f142_5034,f143_5034,f144_5034,f145_5034,f146_5034,f147_5034,f148_5034,f149_5034,f150_5034,
        f151_5034,f152_5034,f153_5034,f154_5034,f155_5034,f156_5034,f157_5034,f158_5034,f159_5034,f160_5034,f161_5034,f162_5034,
        f163_5034,f164_5034
        from wind.tb_object_5034 a            
            inner join
            (select f1_0001,f16_0001,f17_0001
            from wind.tb_object_0001
            where f12_0001='A'
            and f24_0001 is not null
            ) b on a.f1_5034=b.f17_0001
            where a.f3_5034>=%s
            and a.f3_5034<=%s
            """%(start_date,end_date))

#指数相关-----------------------------------------------------------------------       
    #数据hs300_close_weight
    #这张表貌似被废弃了！！！
    table_func.write_table(table_name='hs300_close_weight',query="""
        select ob_object_id,f1_9007 as index_id,f2_9007 as wind_id,f3_9007 as trading_date, f8_9007 as stock_name,
        f11_9007 as close_price,f12_9007 as total_shares,f13_9007,f14_9007,f15_9007 as total_mv,f16_9007,f17_9007,
        f18_9007,f19_9007,f24_9007,f25_9007,f27_9007,f28_9007,f30_9007
        from wind.tb_object_9007
        where f3_9007>=%s
        and f3_9007<=%s
        """%(start_date,end_date))
       
    #数据hs300_next_close_weight
    table_func.write_table(table_name='hs300_next_close_weight',query="""
        select ob_object_id,f1_9008 as index_id,f2_9008 as wind_id,f3_9008 as trading_date, f8_9008 as stock_name,
        f11_9008 as total_shares,f12_9008,f13_9008,f14_9008,f15_9008 as close_price,f16_9008 as adjust_open_price,
        f17_9008 as total_mv,f18_9008,f19_9008,f20_9008,f21_9008,f26_9008,f27_9008,f29_9008,f30_9008
        from wind.tb_object_9008
        where f3_9008>=%s
        and f3_9008<=%s
        """%(start_date,end_date))
       
    #数据index_price
    
    #SELECT distinct f2_1402,f2_1090,b.OB_OBJECT_NAME_1090,F5_1090 from wind.TB_OBJECT_1402 a 
    #left join wind.TB_OBJECT_1090 b on a.F2_1402=b.F2_1090
    #where f5_1090 in ('深圳','上海')
    
    #沪深300(S12426),中证500(S24125),中证800(S24133),上证50(S10442),上证综指(1A0001),深证成指(2A01),创业板指(S3805767),中证全指(S3902554)
    table_func.write_table(table_name='index_price',query="""
        select ob_object_id,f1_1120 as index_id,f2_1120 as trading_date,f4_1120 as last_close,f5_1120 as last_open,
        f6_1120 as last_high,f7_1120 as last_low,f8_1120 as close_price,f9_1120 as volume,f11_1120/10 as amount 
        from wind.tb_object_1120
        where f1_1120 in ('S12426','S24125','S24133','S10442','1A0001','2A01','S3805767','S3902554')
        and f2_1120>=%s
        and f2_1120<=%s
        """%(start_date,end_date))
    #数据index_component
    table_func.write_table(table_name='index_component',query="""
        select ob_object_id,f1_1807 as index_id,f2_1807 as wind_id,f3_1807 as trading_date,f4_1807/100 as weight
        from wind.tb_object_1807
        where f1_1807 in ('S12426','S24125','S24133','S10442','1A0001','2A01','S3805767')
        and f3_1807>=%s
        and f3_1807<=%s
        """%(start_date,end_date))
#配股分红数据---------------------------------------------------------------------
    #数据table_1092
    table_func.write_table(table_name='table_1092',query="""
        select ob_object_id,b.f1_0001 as stock_id,f9_1092,f10_1092,f17_1092,f18_1092,f19_1092,f49_1092,f50_1092,
        f51_1092,f52_1092,f55_1092,f59_1092,f60_1092,f62_1092,F66_1092,F67_1092
        from wind.tb_object_1092 a
            inner join
            (select f1_0001,f16_0001
            from wind.tb_object_0001
            where f12_0001='A'
            and f24_0001 is not null
            ) b on a.f49_1092=b.f16_0001
            where f57_1092>=%s
            and f57_1092<=%s
            """%(start_date,end_date))
           
    #表table_1093
    table_func.write_table(table_name='table_1093',query="""
        select ob_object_id,b.f1_0001 as stock_id,f1_1093,f4_1093,f5_1093,f6_1093,f7_1093,f8_1093,f9_1093,f10_1093,
        f24_1093,f25_1093,f26_1093,f27_1093,f28_1093,f36_1093,f39_1093,f40_1093,f50_1093
        from wind.tb_object_1093 a
            inner join
            (select f1_0001,f16_0001
            from wind.tb_object_0001
            where f12_0001='A'
            and f24_0001 is not null
            ) b on a.f1_1093=b.f16_0001
            where f37_1093=0
            and f36_1093>=%s
            and f36_1093<=%s
            """%(start_date,end_date))

    #表table_1094
    table_func.write_table(table_name='table_1094',query="""
        select ob_object_id,b.f1_0001 as stock_id,f1_1094,f2_1094,f3_1094,f5_1094,f6_1094,f8_1094,f9_1094,f10_1094,f19_1094,
        f22_1094,f27_1094,f35_1094,f36_1094,f41_1094,f50_1094,f53_1094,f54_1094,f76_1094,f114_1094,f125_1094,f127_1094,f128_1094
        from wind.tb_object_1094 a
            inner join
            (select f1_0001,f16_0001
            from wind.tb_object_0001
            where f12_0001='A'
            and f24_0001 is not null
            ) b on a.f1_1094=b.f16_0001
            where f52_1094>=%s
            and f52_1094<=%s
            """%(start_date,end_date))

#一致预期数据--------------------------------------------------------------------
    #表consist_forecast
    table_func.write_table(table_name='consist_forecast',query="""
        select a.ob_object_id,b.f1_0001 as stock_id,f2_1571 as institution,f3_1571 as analyst,f4_1571 as report_date,f5_1571 as forecast_date,
        f6_1571 as eps_diluted,f7_1571,f8_1571,f12_1571,f13_1571,f14_1571,f15_1571,f16_1571,f17_1571,f18_1571,f19_1571,f20_1571,
        f21_1571,f23_1571,f28_1571,f29_1571 as released_date,OB_RELEASE_DATE_1571,f30_1571,f31_1571,f32_1571,f33_1571,f34_1571,f35_1571,
        f37_1571,f38_1571,f39_1571,f40_1571,f41_1571,f42_1571,f43_1571
        from wind.tb_object_1571 a
            inner join
            (select f1_0001,f17_0001
            from wind.tb_object_0001
            where f12_0001='A'
            and f24_0001 is not null
            ) b on a.f1_1571=b.f17_0001
            where f5_1571>=%s
            and f5_1571<=%s
            """%(start_date,end_date))

           
#因子表设计---------------------------------------------------------------------            
def init_factor(factor=None):    
    table_func.create_factor_table(table_name=factor)
    
    
def update_factorbase():
    engine_mysql = create_engine('mysql+pymysql://jinanda@localhost:3306/winddb?charset=utf8',echo=True)
    table_list=sql.read_sql('show tables;',engine_mysql)
    factor_list=pd.read_excel('E:\研究\多因子研究\因子模型\因子文档.xlsx')[['factor','append','origin']]
    factor_list_m=factor_list['factor'][factor_list['append']=='n']          #factor_list_m 代表的是常规因子，只需要用到当月数据就可以计算出结果
    factor_list_nm=factor_list['factor'][factor_list['append']=='y']         #factor_list_nm 代表非常规因子，需要用到超过一个月的数据计算，数据长度就是data字段值
    new_list=factor_list['factor'][~factor_list['factor'].isin(table_list['Tables_in_winddb'])]
    for new in enumerate(new_list):
        init_factor(factor=new[1])
        
    daylist=glc.datelist[100:-1] 
    
    #对常规因子进行计算并导入数据库
    for factor in enumerate(factor_list_m):
        df=table_func.factor_table_primkey(table_name=factor[1])
        df_daylist=df['last_trading_date'].drop_duplicates()
        add_daylist=daylist[(~daylist.isin(df_daylist))&(daylist>=20080201)] 
        for day in add_daylist:
            get_df=getattr(factor_db_raw,factor[1])
            res=get_df(date=day,pool='All')            
            res.to_sql(name=factor[1],con=engine_mysql,if_exists='append',schema='winddb',index=False,chunksize=10000)
    
    #对非常规因子进行计算
    for factor in enumerate(factor_list_nm):
        df=table_func.factor_table_primkey(table_name=factor[1])
        df_daylist=df['last_trading_date'].drop_duplicates()
    
        add_daylist=daylist[(~daylist.isin(df_daylist))&(daylist>=20090401)] #写入数据的数据月份
        if len(add_daylist)==0:
            pass
        else:
            pre_day=add_daylist.iloc[0] #pre_data的数据起点
            #设置第一个数据集，然后进行数据迭代计算因子值
            get_df=getattr(factor_db_raw,factor[1])
            pre_df=get_df(date=pre_day,pool='All',pre_data=True)
            
            for day in add_daylist:
                res=get_df(data=pre_df,pool='All',date=day)
                ####代码有重大问题存在！！！！！
                res[0].dropna(how='all').drop_duplicates().to_sql(name=factor[1],con=engine_mysql,if_exists='append',schema='winddb',index=False,chunksize=10000)
                pre_df=res[1]
        

    
if __name__=='__main__': 
    init_table_update()
    update_database(start_date='20180610')
    import global_config as glc
    update_factorbase()
#update_database()
##
#基础表设计完之后
#开始创建因子表
#重建因子模型
#以事件驱动方式重新构建多因子模型
#同时增加组合交易记录
