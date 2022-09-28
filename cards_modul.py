import jaydebeapi
import pandas as pd

conn = jaydebeapi.connect(
	'oracle.jdbc.driver.OracleDriver',
	'jdbc:oracle:thin:de3hd/bilbobaggins@de-etl.chronosavant.ru:1521/deoracle',
	['de3hd','bilbobaggins'],
	'ojdbc7.jar')
curs = conn.cursor()



def init_cards():
	try:
		curs.execute('''
			CREATE TABLE de3hd.s_09_DWH_DIM_CARDS(
			card_num varchar(128),
			account_num varchar(128),
 			create_dt timestamp,
 			update_dt timestamp)
		''')
	except jaydebeapi.DatabaseError: 
		print('[-] таблицы s_09_DWH_DIM_CARDS уже существует')
	
	curs.execute('''
		INSERT INTO de3hd.s_09_DWH_DIM_CARDS(
			card_num,
			account_num,
			create_dt,
			update_dt
		)select 
			*
		FROM bank.cards
	''')

def del_tables_cards():
	try:
		curs.execute('DROP TABLE de3hd.s_09_DWH_DIM_CARDS')
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


# del_tables_cards()
# init_cards()
# show_data('de3hd.s_09_DWH_DIM_CARDS')

