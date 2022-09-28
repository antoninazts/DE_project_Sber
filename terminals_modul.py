import jaydebeapi
import pandas as pd

conn = jaydebeapi.connect(
	'oracle.jdbc.driver.OracleDriver',
	'jdbc:oracle:thin:de3hd/bilbobaggins@de-etl.chronosavant.ru:1521/deoracle',
	['de3hd','bilbobaggins'],
	'ojdbc7.jar')
curs = conn.cursor()


def init_terminals():
	try:
		curs.execute('''
			CREATE TABLE de3hd.s_09_DWH_DIM_TERMINALS_HIST(
				terminal_id	varchar(128),
				terminal_type varchar(128),
				terminal_city varchar(128),
				terminal_address varchar(128),
				effective_from timestamp default systimestamp,
				effective_to timestamp default (timestamp'9999-12-31 23:59:59'),
				deleted_flg integer default 0
			)
		''')
	except jaydebeapi.DatabaseError: 
		print('[-] таблица s_09_DWH_DIM_TERMINALS_HIST уже существует')

	try:
		curs.execute('''
			CREATE VIEW de3hd.s_09_DWH_DIM_TERMINALS_HIST_v as
			select
				terminal_id,
				terminal_type,
				terminal_city,
				terminal_address
			FROM de3hd.s_09_DWH_DIM_TERMINALS_HIST
			where deleted_flg = 0
			and systimestamp between effective_from and effective_to
		''')
	except jaydebeapi.DatabaseError: 
		print('[-] представление s_09_DWH_DIM_TERMINALS_HIST_v уже существует')


def load_terminals_tmp(terminal_path):
	df = pd.read_excel(terminal_path)
	try:
		curs.execute('DROP TABLE de3hd.s_09_DWH_FACT_TERMINALS')
	except jaydebeapi.DatabaseError:
		print('[-] de3hd.s_09_DWH_FACT_TERMINALS уже удалена')

	curs.execute('''
		CREATE TABLE de3hd.s_09_DWH_FACT_TERMINALS(
			terminal_id	varchar(128),
			terminal_type varchar(128),
			terminal_city varchar(128),
			terminal_address varchar(128)
		)
	''')

	curs.executemany('''
		INSERT INTO de3hd.s_09_DWH_FACT_TERMINALS VALUES(?,?,?,?)
	''', df.values.tolist())


def create_new_terminals():
	curs.execute('''
		CREATE TABLE de3hd.s_09_STG_new_terminals as
			SELECT 
				t1.terminal_id,
				t1.terminal_type,
				t1.terminal_city,
				t1.terminal_address
			FROM de3hd.s_09_DWH_FACT_TERMINALS t1
			left join de3hd.s_09_DWH_DIM_TERMINALS_HIST_v t2
			on t1.terminal_id = t2.terminal_id
			where t2.terminal_id is null
	''')


def create_del_terminals():
	curs.execute('''
		CREATE TABLE de3hd.s_09_STG_del_terminals as
			SELECT 
				t1.terminal_id,
				t1.terminal_type,
				t1.terminal_city,
				t1.terminal_address
			FROM de3hd.s_09_DWH_DIM_TERMINALS_HIST_v t1
			left join de3hd.s_09_DWH_FACT_TERMINALS t2
			on t1.terminal_id = t2.terminal_id
			where t2.terminal_id is null
	''')


def create_updated_terminals():
	curs.execute('''
		CREATE TABLE de3hd.s_09_STG_upd_terminals as
		SELECT
			t2.terminal_id,
			t2.terminal_type,
			t2.terminal_city,
			t2.terminal_address
		FROM de3hd.s_09_DWH_DIM_TERMINALS_HIST_v t1
		inner join de3hd.s_09_DWH_FACT_TERMINALS t2
		on t1.terminal_id = t2.terminal_id
		and  (t1.terminal_type <> t2.terminal_type
		or    t1.terminal_city <> t2.terminal_city
		or    t1.terminal_address <> t2.terminal_address)
	''')


def update_terminals():

	curs.execute('''
		UPDATE de3hd.s_09_DWH_DIM_TERMINALS_HIST
		SET effective_to = systimestamp - INTERVAL '1' MINUTE
		WHERE terminal_id in (select terminal_id FROM de3hd.s_09_STG_upd_terminals)
		and effective_to = (timestamp'9999-12-31 23:59:59')
	''')

	curs.execute('''
		UPDATE de3hd.s_09_DWH_DIM_TERMINALS_HIST
		SET effective_to = systimestamp - INTERVAL '1' MINUTE
		WHERE terminal_id in (select terminal_id FROM de3hd.s_09_STG_del_terminals)
		and effective_to = (timestamp'9999-12-31 23:59:59')
	''')

	curs.execute('''
		INSERT INTO de3hd.s_09_DWH_DIM_TERMINALS_HIST(
			terminal_id,
			terminal_type,
			terminal_city,
			terminal_address
		)select 
			terminal_id,
			terminal_type,
			terminal_city,
			terminal_address
		FROM de3hd.s_09_STG_new_terminals
	''')

	curs.execute('''
		INSERT INTO de3hd.s_09_DWH_DIM_TERMINALS_HIST(
			terminal_id,
			terminal_type,
			terminal_city,
			terminal_address
		)select 
			terminal_id,
			terminal_type,
			terminal_city,
			terminal_address
		FROM de3hd.s_09_STG_upd_terminals
	''')

	curs.execute('''
		INSERT INTO de3hd.s_09_DWH_DIM_TERMINALS_HIST(
			terminal_id,
			terminal_type,
			terminal_city,
			terminal_address,
			deleted_flg
		)select 
			terminal_id,
			terminal_type,
			terminal_city,
			terminal_address,
			1
		FROM de3hd.s_09_STG_del_terminals
	''')


def del_tmp_tables_terminals():
	try:
		curs.execute('DROP TABLE de3hd.s_09_DWH_FACT_TERMINALS')
		curs.execute('DROP TABLE de3hd.s_09_STG_new_terminals')
		curs.execute('DROP TABLE de3hd.s_09_STG_del_terminals')
		curs.execute('DROP TABLE de3hd.s_09_STG_upd_terminals')
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



# init_terminals()
# del_tmp_tables_terminals()
# load_terminals_tmp('data/terminals_01032021.xlsx')
# create_new_terminals()
# create_del_terminals()
# create_updated_terminals()
# update_terminals()
# show_data('de3hd.s_09_DWH_DIM_TERMINALS_HIST')
# show_data('de3hd.s_09_DWH_DIM_TERMINALS_HIST_v')
# show_data('de3hd.s_09_DWH_FACT_TERMINALS')
# show_data('de3hd.s_09_STG_new_terminals')
# show_data('de3hd.s_09_STG_del_terminals')
# show_data('de3hd.s_09_STG_upd_terminals')