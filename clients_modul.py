import jaydebeapi
import pandas as pd

conn = jaydebeapi.connect(
	'oracle.jdbc.driver.OracleDriver',
	'jdbc:oracle:thin:de3hd/bilbobaggins@de-etl.chronosavant.ru:1521/deoracle',
	['de3hd','bilbobaggins'],
	'ojdbc7.jar')
curs = conn.cursor()


def init_CLIENTS():
	try:
		curs.execute('''
			CREATE TABLE de3hd.s_09_DWH_DIM_CLIENTS_HIST(
			client_id varchar(128),
			last_name varchar(128),
			first_name varchar(128),
			patrinymic varchar(128),
			date_of_birth timestamp,
			passport_num varchar(128),
			passport_valid_to timestamp,
			phone varchar(128),
 			effective_from timestamp default systimestamp,
			effective_to timestamp default (timestamp'9999-12-31 23:59:59'),
			deleted_flg integer default 0)
		''')
	except jaydebeapi.DatabaseError: 
		print('[-] таблицы s_09_DWH_DIM_CLIENTS_HIST уже существует')

	try:
		curs.execute('''
			CREATE VIEW de3hd.s_09_DWH_DIM_CLIENTS_HIST_v as
			select
				client_id,
				last_name,
				first_name,
				patrinymic,
				date_of_birth,
				passport_num,
				passport_valid_to,
				phone
			FROM de3hd.s_09_DWH_DIM_CLIENTS_HIST
			where deleted_flg = 0
			and systimestamp between effective_from and effective_to
		''')
	except jaydebeapi.DatabaseError: 
		print('[-] представление s_09_DWH_DIM_CLIENTS_HIST_v уже существует')
	



def load_CLIENTS_tmp():	
	try:
		curs.execute('DROP TABLE de3hd.s_09_DWH_FACT_CLIENTS')
	except jaydebeapi.DatabaseError:
		print('[-] de3hd.s_09_DWH_FACT_CLIENTS уже удалена')

	curs.execute('''
		CREATE TABLE de3hd.s_09_DWH_FACT_CLIENTS(
			client_id varchar(128),
			last_name varchar(128),
			first_name varchar(128),
			patrinymic varchar(128),
			date_of_birth timestamp,
			passport_num varchar(128),
			passport_valid_to timestamp,
			phone varchar(128),
			effective_from timestamp
		)
	''')
	curs.execute('''
		INSERT INTO de3hd.s_09_DWH_FACT_CLIENTS(
			client_id,
			last_name,
			first_name,
			patrinymic,
			date_of_birth,
			passport_num,
			passport_valid_to,
			phone,
			effective_from
		)select 
			CLIENT_ID,
			LAST_NAME,
			FIRST_NAME,
			PATRONYMIC,
			DATE_OF_BIRTH,
			PASSPORT_NUM,
			PASSPORT_VALID_TO,
			PHONE,
			CREATE_DT
		FROM bank.clients
	''')



def create_new_CLIENTS():
	curs.execute('''
		CREATE TABLE de3hd.s_09_STG_new_CLIENTS as
			SELECT 
				t1.client_id,
				t1.last_name,
				t1.first_name,
				t1.patrinymic,
				t1.date_of_birth,
				t1.passport_num,
				t1.passport_valid_to,
				t1.phone
			FROM de3hd.s_09_DWH_FACT_CLIENTS t1
			left join de3hd.s_09_DWH_DIM_CLIENTS_HIST_v t2
			on t1.client_id = t2.client_id
			where t2.client_id is null
	''')


def create_del_CLIENTS():
	curs.execute('''
		CREATE TABLE de3hd.s_09_STG_del_CLIENTS as
			SELECT 
				t1.client_id,
				t1.last_name,
				t1.first_name,
				t1.patrinymic,
				t1.date_of_birth,
				t1.passport_num,
				t1.passport_valid_to,
				t1.phone
			FROM de3hd.s_09_DWH_DIM_CLIENTS_HIST_v t1
			left join de3hd.s_09_DWH_FACT_CLIENTS t2
			on t1.client_id = t2.client_id
			where t2.client_id is null
	''')


def create_upd_CLIENTS():
	curs.execute('''
		CREATE TABLE de3hd.s_09_STG_upd_CLIENTS as
			SELECT
				t2.client_id,
				t2.last_name,
				t2.first_name,
				t2.patrinymic,
				t2.date_of_birth,
				t2.passport_num,
				t2.passport_valid_to,
				t2.phone
			FROM de3hd.s_09_DWH_DIM_CLIENTS_HIST_v t1
			inner join de3hd.s_09_DWH_FACT_CLIENTS t2
			on t1.client_id = t2.client_id
			and  (t1.last_name <> t2.last_name
			or t1.first_name <> t2.first_name
			or t1.patrinymic <> t2.patrinymic
			or t1.date_of_birth <> t2.date_of_birth
			or t1.passport_num <> t2.passport_num
			or t1.passport_valid_to <> t2.passport_valid_to
			or t1.phone <> t2.phone)
		''')



def update_CLIENTS():

	curs.execute('''
		UPDATE de3hd.s_09_DWH_DIM_CLIENTS_HIST
		SET effective_to = systimestamp - INTERVAL '1' MINUTE
		WHERE client_id in (select client_id FROM de3hd.s_09_STG_upd_CLIENTS)
		and effective_to = (timestamp'9999-12-31 23:59:59')
	''')

	curs.execute('''
		UPDATE de3hd.s_09_DWH_DIM_CLIENTS_HIST
		SET effective_to = systimestamp - INTERVAL '1' MINUTE
		WHERE client_id in (select client_id FROM de3hd.s_09_STG_del_CLIENTS)
		and effective_to = (timestamp'9999-12-31 23:59:59')
	''')

	curs.execute('''
		INSERT INTO de3hd.s_09_DWH_DIM_CLIENTS_HIST(
			client_id,
			last_name,
			first_name,
			patrinymic,
			date_of_birth,
			passport_num,
			passport_valid_to,
			phone
		)select 
			client_id,
			last_name,
			first_name,
			patrinymic,
			date_of_birth,
			passport_num,
			passport_valid_to,
			phone
		FROM de3hd.s_09_STG_new_CLIENTS
	''')

	curs.execute('''
		INSERT INTO de3hd.s_09_DWH_DIM_CLIENTS_HIST(
			client_id,
			last_name,
			first_name,
			patrinymic,
			date_of_birth,
			passport_num,
			passport_valid_to,
			phone
		)select 
			client_id,
			last_name,
			first_name,
			patrinymic,
			date_of_birth,
			passport_num,
			passport_valid_to,
			phone
		FROM de3hd.s_09_STG_upd_CLIENTS
	''')

	curs.execute('''
		INSERT INTO de3hd.s_09_DWH_DIM_CLIENTS_HIST(
			client_id,
			last_name,
			first_name,
			patrinymic,
			date_of_birth,
			passport_num,
			passport_valid_to,
			phone,
			deleted_flg
		)select 
			client_id,
			last_name,
			first_name,
			patrinymic,
			date_of_birth,
			passport_num,
			passport_valid_to,
			phone,
			1
		FROM de3hd.s_09_STG_del_CLIENTS
	''')



def del_tmp_tables_CLIENTS():
	try:
		curs.execute('DROP TABLE de3hd.s_09_DWH_FACT_CLIENTS')
		curs.execute('DROP TABLE de3hd.s_09_STG_new_CLIENTS')
		curs.execute('DROP TABLE de3hd.s_09_STG_del_CLIENTS')
		curs.execute('DROP TABLE de3hd.s_09_STG_upd_CLIENTS')
	except jaydebeapi.DatabaseError: 
		print('[-] временные таблицы уже удалены')

# def show_data(source):
# 	curs.execute(f'SELECT * FROM {source}')
# 	print('-'*20)
# 	print(source)
# 	print('-'*20)
# 	for row in curs.fetchall():
# 		print(*row)
# 	print('\n')


# init_CLIENTS()
# del_tmp_tables_CLIENTS()
# load_CLIENTS_tmp()
# create_new_CLIENTS()
# create_del_CLIENTS()
# create_upd_CLIENTS()
# update_CLIENTS()
# show_data('de3hd.s_09_DWH_DIM_CLIENTS_HIST')
# show_data('de3hd.s_09_DWH_DIM_CLIENTS_HIST_v')
# show_data('de3hd.s_09_DWH_FACT_CLIENTS')
# show_data('de3hd.s_09_STG_new_CLIENTS')
# show_data('de3hd.s_09_STG_del_CLIENTS')
# show_data('de3hd.s_09_STG_upd_CLIENTS')