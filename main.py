import jaydebeapi
import pandas as pd
from sys import argv
import os

import transactions_modul
import terminals_modul
import passport_blacklist_modul
import cards_modul
import accounts_modul
import clients_modul
import fraud_modul

conn = jaydebeapi.connect(
	'oracle.jdbc.driver.OracleDriver',
	'jdbc:oracle:thin:de3hd/bilbobaggins@de-etl.chronosavant.ru:1521/deoracle',
	['de3hd','bilbobaggins'],
	'ojdbc7.jar')
curs = conn.cursor()


for root, dirs, files in os.walk('data/'):
	for filename in files:
		if filename.startswith('transactions'):
			trans_path = filename
			# print('trans_path = ' + trans_path)
		elif filename.startswith('terminals'):
			terminal_path = filename
			# print('terminal_path = ' + terminal_path)
		elif filename.startswith('passport_blacklist'):
			passport_blacklist_path = filename
			# print('passport_blacklist_path = ' + passport_blacklist_path)

curs.execute('''ALTER SESSION SET NLS_DATE_FORMAT = 'YYYY-MM-DD HH24:MI:SS' ''')


def cards():
	# cards_modul.del_tables_cards()
	cards_modul.init_cards()
	# cards_modul.show_data('de3hd.s_09_DWH_DIM_CARDS')


def accounts():
	# accounts_modul.del_tables_accounts()
	accounts_modul.init_accounts()
	# accounts_modul.show_data('de3hd.s_09_DWH_DIM_ACCOUNTS')


def clients():
	clients_modul.init_CLIENTS()
	clients_modul.del_tmp_tables_CLIENTS()
	clients_modul.load_CLIENTS_tmp()
	clients_modul.create_new_CLIENTS()
	clients_modul.create_del_CLIENTS()
	clients_modul.create_upd_CLIENTS()
	clients_modul.update_CLIENTS()
	# show_data('de3hd.s_09_DWH_DIM_CLIENTS_HIST')
	# show_data('de3hd.s_09_DWH_DIM_CLIENTS_HIST_v')
	# show_data('de3hd.s_09_DWH_FACT_CLIENTS')
	# show_data('de3hd.s_09_STG_new_CLIENTS')
	# show_data('de3hd.s_09_STG_del_CLIENTS')
	# show_data('de3hd.s_09_STG_upd_CLIENTS')


def transactions(trans_path):
	transactions_modul.init_transactions()
	transactions_modul.del_tmp_tables_transactions()
	# transactions_modul.load_transactions_tmp('data/transactions_01032021.txt')
	transactions_modul.load_transactions_tmp('data/'+trans_path)
	transactions_modul.update_transactions()
	# show_data('de3hd.s_09_DWH_FACT_TRANSACTIONS')
	# show_data('de3hd.s_09_DWH_DIM_TRANSACTIONS')


def terminals(terminal_path):
	terminals_modul.init_terminals()
	terminals_modul.del_tmp_tables_terminals()
	# terminals_modul.load_terminals_tmp('data/terminals_01032021.xlsx')
	terminals_modul.load_terminals_tmp('data/'+terminal_path)
	terminals_modul.create_new_terminals()
	terminals_modul.create_del_terminals()
	terminals_modul.create_updated_terminals()
	terminals_modul.update_terminals()
	# show_data('de3hd.s_09_DWH_DIM_TERMINALS_HIST')
	# show_data('de3hd.s_09_DWH_DIM_TERMINALS_HIST_v')
	# show_data('de3hd.s_09_DWH_FACT_TERMINALS')
	# show_data('de3hd.s_09_STG_new_terminals_tmp')
	# show_data('de3hd.s_09_STG_del_terminals_tmp')
	# show_data('de3hd.s_09_STG_upd_terminals_tmp')


def passport_blacklist(passport_blacklist_path):
	passport_blacklist_modul.init_PSSPRT_BL()
	passport_blacklist_modul.del_tmp_tables_PSSPRT_BL()
	# passport_blacklist_modul.load_PSSPRT_BL_tmp('data/passport_blacklist_01032021.xlsx')
	passport_blacklist_modul.load_PSSPRT_BL_tmp('data/'+passport_blacklist_path)
	passport_blacklist_modul.create_new_PSSPRT_BL()
	passport_blacklist_modul.create_del_PSSPRT_BL()
	passport_blacklist_modul.create_upd_PSSPRT_BL()
	passport_blacklist_modul.update_PSSPRT_BL()
	# show_data('de3hd.s_09_DWH_DIM_PSSPRT_BL_HIST')
	# show_data('de3hd.s_09_DWH_DIM_PSSPRT_BL_HIST_v')
	# show_data('de3hd.s_09_DWH_FACT_PSSPRT_BL')
	# show_data('de3hd.s_09_STG_new_PSSPRT_BL')
	# show_data('de3hd.s_09_STG_del_PSSPRT_BL')
	# show_data('de3hd.s_09_STG_upd_PSSPRT_BL')


def fraud():
	fraud_modul.init_REP_FRAUD()
	fraud_modul.del_tmp_tables_fraud()
	fraud_modul.find_FRAUD_1()
	fraud_modul.find_FRAUD_2()
	fraud_modul.find_FRAUD_3()
	# fraud_modul.find_FRAUD_4()
	fraud_modul.update_rep_fraud()
	fraud_modul.show_data('de3hd.s_09_REP_FRAUD')


def show_data(source):
	curs.execute('SELECT * FROM {0}'.format(source))
	print('-'*30)
	print(source)
	print('-'*30)
	for row in curs.fetchall():
		print(*row)
	print('\n')


def rename_input_files():
	os.rename('data/'+trans_path, 'archive/'+trans_path+'.backup')
	os.rename('data/'+terminal_path, 'archive/'+terminal_path+'.backup')
	os.rename('data/'+passport_blacklist_path, 'archive/'+passport_blacklist_path+'.backup')


def del_tmp_tables_main():
	curs.execute('DROP TABLE de3hd.s_09_DWH_FACT_TRANSACTIONS')
	curs.execute('DROP TABLE de3hd.s_09_DWH_FACT_TERMINALS')
	curs.execute('DROP TABLE de3hd.s_09_STG_new_terminals')
	curs.execute('DROP TABLE de3hd.s_09_STG_del_terminals')
	curs.execute('DROP TABLE de3hd.s_09_STG_upd_terminals')
	curs.execute('DROP TABLE de3hd.s_09_DWH_FACT_PSSPRT_BL')
	curs.execute('DROP TABLE de3hd.s_09_STG_new_PSSPRT_BL')
	curs.execute('DROP TABLE de3hd.s_09_STG_del_PSSPRT_BL')
	curs.execute('DROP TABLE de3hd.s_09_STG_upd_PSSPRT_BL')
	curs.execute('DROP TABLE de3hd.s_09_DWH_FACT_CLIENTS')
	curs.execute('DROP TABLE de3hd.s_09_STG_new_CLIENTS')
	curs.execute('DROP TABLE de3hd.s_09_STG_del_CLIENTS')
	curs.execute('DROP TABLE de3hd.s_09_STG_upd_CLIENTS')
	curs.execute('DROP TABLE de3hd.s_09_STG_FRAUD_1')
	curs.execute('DROP TABLE de3hd.s_09_STG_FRAUD_2')
	curs.execute('DROP TABLE de3hd.s_09_STG_FRAUD_3')


# def del_all_tables_main():
	# curs.execute('DROP TABLE de3hd.s_09_DWH_DIM_TRANSACTIONS')
	# curs.execute('DROP TABLE de3hd.s_09_DWH_DIM_TERMINALS_HIST')
	# curs.execute('DROP TABLE de3hd.s_09_DWH_DIM_TERMINALS_HIST_v')
	# curs.execute('DROP TABLE de3hd.s_09_DWH_DIM_PSSPRT_BL_HIST')
	# curs.execute('DROP TABLE de3hd.s_09_DWH_DIM_PSSPRT_BL_HIST_v')
	# curs.execute('DROP TABLE de3hd.s_09_DWH_DIM_CARDS')
	# curs.execute('DROP TABLE de3hd.s_09_DWH_DIM_ACCOUNTS')
	# curs.execute('DROP TABLE de3hd.s_09_DWH_DIM_CLIENTS_HIST')
	# curs.execute('DROP TABLE de3hd.s_09_DWH_DIM_CLIENTS_HIST_v')
	# curs.execute('DROP TABLE de3hd.s_09_DWH_FACT_TRANSACTIONS')
	# curs.execute('DROP TABLE de3hd.s_09_DWH_FACT_TERMINALS')
	# curs.execute('DROP TABLE de3hd.s_09_STG_new_terminals')
	# curs.execute('DROP TABLE de3hd.s_09_STG_del_terminals')
	# curs.execute('DROP TABLE de3hd.s_09_STG_upd_terminals')
	# curs.execute('DROP TABLE de3hd.s_09_DWH_FACT_PSSPRT_BL')
	# curs.execute('DROP TABLE de3hd.s_09_STG_new_PSSPRT_BL')
	# curs.execute('DROP TABLE de3hd.s_09_STG_del_PSSPRT_BL')
	# curs.execute('DROP TABLE de3hd.s_09_STG_upd_PSSPRT_BL')
	# curs.execute('DROP TABLE de3hd.s_09_DWH_FACT_CLIENTS')
	# curs.execute('DROP TABLE de3hd.s_09_STG_new_CLIENTS')
	# curs.execute('DROP TABLE de3hd.s_09_STG_del_CLIENTS')
	# curs.execute('DROP TABLE de3hd.s_09_STG_upd_CLIENTS')
	# curs.execute('DROP TABLE de3hd.s_09_REP_FRAUD')
	# curs.execute('DROP TABLE de3hd.s_09_STG_FRAUD_1')
	# curs.execute('DROP TABLE de3hd.s_09_STG_FRAUD_2')
	# curs.execute('DROP TABLE de3hd.s_09_STG_FRAUD_3')
	# curs.execute('DROP TABLE de3hd.s_09_STG_FRAUD_4')



# //////////////////// в ы з о в ы   ф у н к ц и й  ///////////////////////////////////////////////////////////////////////

# раскомментировать

# cards()
# accounts()
# clients()
# transactions(trans_path)
# terminals(terminal_path)
# passport_blacklist(passport_blacklist_path)
# fraud()
# show_data('de3hd.s_09_REP_FRAUD')
# rename_input_files()
# del_tmp_tables_main()



# //////////////////// для отладки ///////////////////////////////////////////////////////////////////////


# show_data('de3hd.s_09_DWH_DIM_TRANSACTIONS')
# show_data('de3hd.s_09_DWH_DIM_TERMINALS_HIST_v')
# show_data('de3hd.s_09_DWH_DIM_PSSPRT_BL_HIST_v')
# show_data('de3hd.s_09_DWH_DIM_CARDS')
# show_data('de3hd.s_09_DWH_DIM_ACCOUNTS')
# show_data('de3hd.s_09_DWH_DIM_CLIENTS_HIST_v')
# del_all_tables_main()

