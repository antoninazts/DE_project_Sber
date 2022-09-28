import jaydebeapi
import pandas as pd

conn = jaydebeapi.connect(
	'oracle.jdbc.driver.OracleDriver',
	'jdbc:oracle:thin:de3hd/bilbobaggins@de-etl.chronosavant.ru:1521/deoracle',
	['de3hd','bilbobaggins'],
	'ojdbc7.jar')
curs = conn.cursor()


def init_transactions():
	try:
		curs.execute('''
			CREATE TABLE de3hd.s_09_DWH_DIM_TRANSACTIONS(
				trans_id integer,
				trans_date varchar(128),
				amt decimal,
				card_num varchar(128),
				oper_type varchar(128),
				oper_result varchar(128),
				terminal varchar(128),
				create_dt timestamp default systimestamp,
				update_dt timestamp default systimestamp
			)
		''')
	except jaydebeapi.DatabaseError: 
		print('[-] таблица s_09_DWH_DIM_TRANSACTIONS уже существует')


def load_transactions_tmp(trans_path):
	df = pd.read_csv(trans_path, sep=';', decimal=',' )
	try:
		curs.execute('DROP TABLE de3hd.s_09_DWH_FACT_TRANSACTIONS')
	except jaydebeapi.DatabaseError:
		print('[-] de3hd.s_09_DWH_FACT_TRANSACTIONS уже удалена')

	curs.execute('''
		CREATE TABLE de3hd.s_09_DWH_FACT_TRANSACTIONS(
			trans_id integer,
			trans_date varchar(128),
			amt decimal,
			card_num varchar(128),
			oper_type varchar(128),
			oper_result varchar(128),
			terminal varchar(128)
		)
	''')

	df['transaction_date'] = df['transaction_date'].astype(str)

	curs.executemany('''
		INSERT INTO de3hd.s_09_DWH_FACT_TRANSACTIONS VALUES(?,?,?,?,?,?,?)
	''', df.values.tolist())


def update_transactions():
	curs.execute('''
		INSERT INTO de3hd.s_09_DWH_DIM_TRANSACTIONS(
			trans_id,
			trans_date,
			amt,
			card_num,
			oper_type, 
			oper_result,
			terminal
		)select 
			trans_id,
			trans_date,
			amt,
			card_num,
			oper_type, 
			oper_result,
			terminal
		from de3hd.s_09_DWH_FACT_TRANSACTIONS
	''')


def del_tmp_tables_transactions():
	try:
		curs.execute('DROP TABLE de3hd.s_09_DWH_FACT_TRANSACTIONS')
	except jaydebeapi.DatabaseError: 
		print('[-] временные таблицы уже удалены')


# def show_data(source):
# 		curs.execute(f'SELECT * FROM {source}')
# 		print('-'*20)
# 		print(source)
# 		print('-'*20)
# 		for row in curs.fetchall():
# 			print(*row)
# 		print('\n')


# init_transactions()
# del_tmp_tables_transactions()
# load_transactions_tmp('data/transactions_01032021.txt')
# update_transactions()
# show_data('de3hd.s_09_DWH_FACT_TRANSACTIONS')
# show_data('de3hd.s_09_DWH_DIM_TRANSACTIONS')

