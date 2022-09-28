import jaydebeapi
import pandas as pd

import transactions_modul
import terminals_modul
import passport_blacklist_modul
import cards_modul
import accounts_modul
import clients_modul

conn = jaydebeapi.connect(
	'oracle.jdbc.driver.OracleDriver',
	'jdbc:oracle:thin:de3hd/bilbobaggins@de-etl.chronosavant.ru:1521/deoracle',
	['de3hd','bilbobaggins'],
	'ojdbc7.jar')
curs = conn.cursor()

# curs.execute('''alter session set nls_date_format = 'YYYY-MM-DD HH24:MI:SS' ''')


def init_REP_FRAUD():
	try:
		curs.execute('''
			CREATE TABLE de3hd.s_09_REP_FRAUD(
				event_dt varchar(128),
				passport varchar(128),
				fio varchar(128),
				phone varchar(128),
				event_type varchar(128),
				report_dt timestamp default systimestamp
			)
		''')
	except jaydebeapi.DatabaseError: 
		print('[-] таблица s_09_REP_FRAUD уже существует')


# Признаки мошеннических операций.

# 1. Совершение операции при просроченном или заблокированном паспорте.
def find_FRAUD_1():
	try:
		curs.execute('''
					CREATE TABLE de3hd.s_09_STG_FRAUD_1(
						event_dt varchar(128),
						passport varchar(128),
						fio varchar(128),
						phone varchar(128),
						event_type varchar(128),
						report_dt timestamp default systimestamp
					)
				''')
	except jaydebeapi.DatabaseError: 
		print('[-] таблица s_09_STG_FRAUD_1 уже существует')
	curs.execute('''
		INSERT INTO de3hd.s_09_STG_FRAUD_1(
			event_dt,
			passport,
			fio,
			phone,
			event_type,
			report_dt
		)SELECT
			t1.trans_date as event_dt,
			t4.passport_num as passport,
			t4.last_name||' '||t4.first_name||' '||t4.patrinymic as fio,
			t4.phone as phone,
			'1.Совершение операции при просроченном или заблокированном паспорте.' as event_type,
			to_char(systimestamp, 'DD-MM-YYYY HH24:MI:SS') as report_dt
		FROM de3hd.s_09_DWH_DIM_TRANSACTIONS t1
		inner join de3hd.s_09_DWH_DIM_CARDS t2 ON replace(t1.card_num, ' ', '') = replace(t2.card_num, ' ', '')
		inner join de3hd.s_09_DWH_DIM_ACCOUNTS t3 on t2.account_num = t3.account_num
		inner join de3hd.s_09_DWH_DIM_CLIENTS_HIST_v t4 on t3.client = t4.client_id
		WHERE t4.passport_valid_to < to_timestamp(t1.trans_date, 'YYYY-MM-DD HH24:MI:SS')
		or t4.passport_num in (SELECT passport_num FROM de3hd.s_09_DWH_DIM_PSSPRT_BL_HIST_v)
		''')

	# for row in curs.fetchmany(size=5):
	# 	print(*row)



# 2.Совершение операции при недействующем договоре.
def find_FRAUD_2():
	try:
		curs.execute('''
					CREATE TABLE de3hd.s_09_STG_FRAUD_2(
						event_dt varchar(128),
						passport varchar(128),
						fio varchar(128),
						phone varchar(128),
						event_type varchar(128),
						report_dt timestamp default systimestamp
					)
				''')
	except jaydebeapi.DatabaseError: 
		print('[-] таблица s_09_STG_FRAUD_2 уже существует')
	curs.execute('''
		INSERT INTO de3hd.s_09_STG_FRAUD_2(
			event_dt,
			passport,
			fio,
			phone,
			event_type,
			report_dt
		)SELECT
			t1.trans_date as event_dt,
			t4.passport_num as passport,
			t4.last_name||' '||t4.first_name||' '||t4.patrinymic as fio,
			t4.phone as phone,
			'2.Совершение операции при недействующем договоре.' as event_type,
			to_char(systimestamp, 'DD-MM-YYYY HH24:MI:SS') as report_dt
		FROM de3hd.s_09_DWH_DIM_TRANSACTIONS t1
		inner join de3hd.s_09_DWH_DIM_CARDS t2 ON replace(t1.card_num, ' ', '') = replace(t2.card_num, ' ', '')
		inner join de3hd.s_09_DWH_DIM_ACCOUNTS t3 on t2.account_num = t3.account_num
		inner join de3hd.s_09_DWH_DIM_CLIENTS_HIST_v t4 on t3.client = t4.client_id
		WHERE t3.valid_to < to_timestamp(t1.trans_date, 'YYYY-MM-DD HH24:MI:SS')
		''')


# 3.Совершение операций в разных городах в течение одного часа.
def find_FRAUD_3():
	try:
		curs.execute('''
			CREATE TABLE de3hd.s_09_STG_FRAUD_3(
				event_dt varchar(128),
				passport varchar(128),
				fio varchar(128),
				phone varchar(128),
				event_type varchar(128),
				report_dt timestamp default systimestamp
			)
		''')
	except jaydebeapi.DatabaseError: 
		print('[-] таблица s_09_STG_FRAUD_3 уже существует')

	curs.execute('''
		INSERT INTO de3hd.s_09_STG_FRAUD_3(
			event_dt,
			passport,
			fio,
			phone,
			event_type,
			report_dt
		)SELECT
			event_dt,
 			passport,
 			fio,
 			phone,
 			event_type,
 			report_dt
 		FROM(
			SELECT 
				city_lag,
				city,
				city_lead,
				event_dt_lag,
				event_dt,
				event_dt_lead,
				passport,
	 			fio,
	 			phone,
	 			event_type,
	 			report_dt
			FROM(
				SELECT
					t5.terminal_city as city,
					lag(t5.terminal_city) over(partition by t1.card_num order by t1.card_num) as city_lag,
					lead(t5.terminal_city) over(partition by t1.card_num order by t1.card_num) as city_lead,
					to_timestamp(t1.trans_date, 'YYYY-MM-DD HH24:MI:SS') as event_dt,
					to_timestamp(lag(t1.trans_date) over(partition by t1.card_num order by t1.card_num), 'YYYY-MM-DD HH24:MI:SS') + INTERVAL '1' HOUR as event_dt_lag,
					to_timestamp(lead(t1.trans_date) over(partition by t1.card_num order by t1.card_num), 'YYYY-MM-DD HH24:MI:SS') - INTERVAL '1' HOUR as event_dt_lead,
					t4.passport_num as passport,
					t4.last_name||' '||t4.first_name||' '||t4.patrinymic as fio,
					t4.phone as phone,
					'3.Совершение операций в разных городах в течение одного часа.' as event_type,
					to_char(systimestamp, 'DD-MM-YYYY HH24:MI:SS') as report_dt
				FROM de3hd.s_09_DWH_DIM_TRANSACTIONS t1
				inner join de3hd.s_09_DWH_DIM_CARDS t2 ON replace(t1.card_num, ' ', '') = replace(t2.card_num, ' ', '')
				inner join de3hd.s_09_DWH_DIM_ACCOUNTS t3 on t2.account_num = t3.account_num
				inner join de3hd.s_09_DWH_DIM_CLIENTS_HIST_v t4 on t3.client = t4.client_id
				inner join de3hd.s_09_DWH_DIM_TERMINALS_HIST_v t5 on t5.terminal_id = t1.terminal
				)
				WHERE (city_lag != city and event_dt_lag > event_dt) or (city_lead != city and event_dt_lead < event_dt)
			)
		''')
	# for row in curs.fetchmany(size=50):
	# 	print(*row)




# 4. Попытка подбора суммы. В течение 20 минут проходит более 3х операций
# со следующим шаблоном – каждая последующая меньше предыдущей, при этом отклонены все кроме последней. Последняя операция (успешная) в такой цепочке считается мошеннической.



def update_rep_fraud():
	curs.execute('''
			INSERT INTO de3hd.s_09_REP_FRAUD(
				event_dt,
				passport,
				fio,
				phone,
				event_type,
				report_dt
			)SELECT
				event_dt,
				passport,
				fio,
				phone,
				event_type,
				report_dt
			FROM de3hd.s_09_STG_FRAUD_1
		''')
	curs.execute('''
			INSERT INTO de3hd.s_09_REP_FRAUD(
				event_dt,
				passport,
				fio,
				phone,
				event_type,
				report_dt
			)SELECT
				event_dt,
				passport,
				fio,
				phone,
				event_type,
				report_dt
			FROM de3hd.s_09_STG_FRAUD_2
		''')
	# curs.execute('''
	# 	INSERT INTO de3hd.s_09_REP_FRAUD(
	# 		event_dt,
	# 		passport,
	# 		fio,
	# 		phone,
	# 		event_type,
	# 		report_dt
	# 	)SELECT
	# 		event_dt,
	# 		passport,
	# 		fio,
	# 		phone,
	# 		event_type,
	# 		report_dt
	# 	FROM de3hd.s_09_STG_FRAUD_3
	# ''')
	# curs.execute('''
	# 	INSERT INTO de3hd.s_09_REP_FRAUD(
	# 		event_dt,
	# 		passport,
	# 		fio,
	# 		phone,
	# 		event_type,
	# 		report_dt
	# 	)SELECT
	# 		event_dt,
	# 		passport,
	# 		fio,
	# 		phone,
	# 		event_type,
	# 		report_dt
	# 	FROM de3hd.s_09_STG_FRAUD_4
	# ''')


def del_tmp_tables_fraud():
	try:
		curs.execute('DROP TABLE de3hd.s_09_STG_FRAUD_1')
		curs.execute('DROP TABLE de3hd.s_09_STG_FRAUD_2')
		curs.execute('DROP TABLE de3hd.s_09_STG_FRAUD_3')
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



# init_REP_FRAUD()
# del_tmp_tables_fraud()
# find_FRAUD_1()
# find_FRAUD_2()
# find_FRAUD_3()
# show_data('de3hd.s_09_STG_FRAUD_1')
# show_data('de3hd.s_09_STG_FRAUD_2')
# show_data('de3hd.s_09_STG_FRAUD_3')
# update_rep_fraud()
# show_data('de3hd.s_09_REP_FRAUD')