import jaydebeapi
import pandas as pd

conn = jaydebeapi.connect(
	'oracle.jdbc.driver.OracleDriver',
	'jdbc:oracle:thin:de3hd/bilbobaggins@de-etl.chronosavant.ru:1521/deoracle',
	['de3hd','bilbobaggins'],
	'ojdbc7.jar')
curs = conn.cursor()


def init_PSSPRT_BL():
	try:
		curs.execute('''
			CREATE TABLE de3hd.s_09_DWH_DIM_PSSPRT_BL_HIST(
				entry_dt varchar(128),
				passport_num varchar(128),
				effective_from timestamp default systimestamp,
				effective_to timestamp default (timestamp'9999-12-31 23:59:59'),
				deleted_flg integer default 0
			)
		''')
	except jaydebeapi.DatabaseError: 
		print('[-] таблица s_09_DWH_DIM_PSSPRT_BL_HIST уже существует')

	try:
		curs.execute('''
			CREATE VIEW de3hd.s_09_DWH_DIM_PSSPRT_BL_HIST_v as
			select
				entry_dt,
				passport_num
			FROM de3hd.s_09_DWH_DIM_PSSPRT_BL_HIST
			where deleted_flg = 0
			and systimestamp between effective_from and effective_to
		''')
	except jaydebeapi.DatabaseError: 
		print('[-] представление s_09_DWH_DIM_PSSPRT_BL_HIST_v уже существует')


def load_PSSPRT_BL_tmp(passport_blacklist_path):
	df = pd.read_excel(passport_blacklist_path)
	try:
		curs.execute('DROP TABLE de3hd.s_09_DWH_FACT_PSSPRT_BL')
	except jaydebeapi.DatabaseError:
		print('[-] de3hd.s_09_DWH_FACT_PSSPRT_BL уже удалена')

	curs.execute('''
		CREATE TABLE de3hd.s_09_DWH_FACT_PSSPRT_BL(
			entry_dt varchar(128),
			passport_num varchar(128)
		)
	''')
	# print(df.values.tolist())
	# print(df.astype(str).values.tolist())

	df['date'] = df['date'].astype(str)

	curs.executemany('''
		INSERT INTO de3hd.s_09_DWH_FACT_PSSPRT_BL VALUES(?,?)
	''', df.values.tolist())

	# curs.executemany('''
	# 	INSERT INTO de3hd.s_09_DWH_FACT_PSSPRT_BL VALUES(?,?)
	# ''', df.astype(str).values.tolist())


def create_new_PSSPRT_BL():
	curs.execute('''
		CREATE TABLE de3hd.s_09_STG_new_PSSPRT_BL as
			SELECT 
				t1.entry_dt,
				t1.passport_num
			FROM de3hd.s_09_DWH_FACT_PSSPRT_BL t1
			left join de3hd.s_09_DWH_DIM_PSSPRT_BL_HIST_v t2
			on t1.passport_num = t2.passport_num
			where t2.passport_num is null
	''')


def create_del_PSSPRT_BL():
	curs.execute('''
		CREATE TABLE de3hd.s_09_STG_del_PSSPRT_BL as
			SELECT 
				t1.entry_dt,
				t1.passport_num
			FROM de3hd.s_09_DWH_DIM_PSSPRT_BL_HIST_v t1
			left join de3hd.s_09_DWH_FACT_PSSPRT_BL t2
			on t1.passport_num = t2.passport_num
			where t2.passport_num is null
	''')


def create_upd_PSSPRT_BL():
	curs.execute('''
		CREATE TABLE de3hd.s_09_STG_upd_PSSPRT_BL as
			SELECT
				t2.entry_dt,
				t2.passport_num
			FROM de3hd.s_09_DWH_DIM_PSSPRT_BL_HIST_v t1
			inner join de3hd.s_09_DWH_FACT_PSSPRT_BL t2
			on t1.passport_num = t2.passport_num
			and  t1.entry_dt <> t2.entry_dt
	''')


def update_PSSPRT_BL():

	curs.execute('''
		UPDATE de3hd.s_09_DWH_DIM_PSSPRT_BL_HIST
		SET effective_to = systimestamp - INTERVAL '1' MINUTE
		WHERE passport_num in (select passport_num FROM de3hd.s_09_STG_upd_PSSPRT_BL)
		and effective_to = (timestamp'9999-12-31 23:59:59')
	''')

	curs.execute('''
		UPDATE de3hd.s_09_DWH_DIM_PSSPRT_BL_HIST
		SET effective_to = systimestamp - INTERVAL '1' MINUTE
		WHERE passport_num in (select passport_num FROM de3hd.s_09_STG_del_PSSPRT_BL)
		and effective_to = (timestamp'9999-12-31 23:59:59')
	''')

	curs.execute('''
		INSERT INTO de3hd.s_09_DWH_DIM_PSSPRT_BL_HIST(
			entry_dt,
			passport_num
		)select 
			entry_dt,
			passport_num
		FROM de3hd.s_09_STG_new_PSSPRT_BL
	''')

	curs.execute('''
		INSERT INTO de3hd.s_09_DWH_DIM_PSSPRT_BL_HIST(
			entry_dt,
			passport_num
		)select 
			entry_dt,
			passport_num
		FROM de3hd.s_09_STG_upd_PSSPRT_BL
	''')

	curs.execute('''
		INSERT INTO de3hd.s_09_DWH_DIM_PSSPRT_BL_HIST(
			entry_dt,
			passport_num,
			deleted_flg
		)select 
			entry_dt,
			passport_num,
			1
		FROM de3hd.s_09_STG_del_PSSPRT_BL
	''')


def del_tmp_tables_PSSPRT_BL():
	try:
		curs.execute('DROP TABLE de3hd.s_09_DWH_FACT_PSSPRT_BL')
		curs.execute('DROP TABLE de3hd.s_09_STG_new_PSSPRT_BL')
		curs.execute('DROP TABLE de3hd.s_09_STG_del_PSSPRT_BL')
		curs.execute('DROP TABLE de3hd.s_09_STG_upd_PSSPRT_BL')
	except jaydebeapi.DatabaseError: 
		print('[-] временные таблицы уже удалены')


# def show_data(source):
# 		curs.execute(f'SELECT * FROM {source}')
# 		print('-'*20)
# 		print(source)
# 		print('-'*20)
# 		for row in curs.fetchall():
# 			print(row)
# 		print('\n')


# init_PSSPRT_BL()
# del_tmp_tables_PSSPRT_BL()
# load_PSSPRT_BL_tmp('data/passport_blacklist_03032021.xlsx')
# # load_PSSPRT_BL_tmp('data/passport_blacklist_02032021.xlsx')
# # load_PSSPRT_BL_tmp('data/passport_blacklist_03032021.xlsx')
# create_new_PSSPRT_BL()
# create_del_PSSPRT_BL()
# create_upd_PSSPRT_BL()
# update_PSSPRT_BL()
# show_data('de3hd.s_09_DWH_DIM_PSSPRT_BL_HIST')
# show_data('de3hd.s_09_DWH_DIM_PSSPRT_BL_HIST_v')
# show_data('de3hd.s_09_DWH_FACT_PSSPRT_BL')
# show_data('de3hd.s_09_STG_new_PSSPRT_BL')
# show_data('de3hd.s_09_STG_del_PSSPRT_BL')
# show_data('de3hd.s_09_STG_upd_PSSPRT_BL')
