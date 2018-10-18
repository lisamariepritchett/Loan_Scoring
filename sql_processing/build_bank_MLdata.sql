drop database bank;
create database bank;

## CREATE LOANS TABLE
drop table if exists bank.loans;
create table bank.loans(
        loan_id int primary key ,
        account_id int,
        date_ date,
        amount int,
        duration int,
        payments float(10,2),
        status_ char(3));
truncate table bank.loans;

load data local infile
'/bank_data_loan_default/loan.asc'
into table bank.loans
character set UTF8
fields terminated by ';'
lines terminated by '\n'
ignore 1 lines
(loan_id, account_id, @date_, amount, duration, payments, status_)
set date_ = str_to_date(@date_, '%y%m%d');

alter table bank.loans
add current char(1),
add good_status char(1);  # Add current & good_status

update bank.loans
set status_ = replace(status_, '"', ''),
    current = if(status_='C' OR status_='D', 1, 0),
    good_status = if(status_='A' OR status_='C', 1, 0);

## CREATE ACCOUNTS TABLE
drop table if exists bank.accounts;
create table bank.accounts(
        account_id int(15),
        district_id int(5),
        frequency varchar(20),
        date_ date);
load data local infile
'/bank_data_loan_default/account.asc'
into table bank.accounts
character set UTF8
fields terminated by ';'
lines terminated by '\n'
ignore 1 lines
(account_id, district_id, frequency, @date_)
set date_ = str_to_date(@date_, '%y%m%d');
update bank.accounts
set frequency = replace(replace(replace(
        frequency, '"POPLATEK MESICNE"','monthly')
                        , '"POPLATEK TYDNE"', 'weekly')
                        , '"POPLATEK PO OBRATU"', 'after_transaction');


## CREATE DISP
drop table if exists bank.disp;
create table bank.disp(
        disp_id int,
        client_id int,
        account_id int,
        type_ varchar(12));
load data local infile
'/bank_data_loan_default/disp.asc'
into table bank.disp
character set UTF8
fields terminated by ';'
lines terminated by '\n'
ignore 1 lines;
update bank.disp
set type_ = replace(replace(replace(
                      type_, '\r','')
                      , '"', '')
                      , 'DISPONENT','USER');


###### CLIENTS #######
## CREATE CLIENT_ source table
drop table if exists bank.client_;
create table bank.client_(
        client_id int primary key ,
        birth_number varchar(10),
        district_id smallint);
load data local infile
'/bank_data_loan_default/client.asc'
into table bank.client_
character set UTF8
fields terminated by ';'
lines terminated by '\n'
ignore 1 lines;

## CREATE CLIENT TABLE with Sex pulled out from birth_number
drop table if exists bank.clients;
create table bank.clients as
select bank.client_.client_id, district_id, birth_number, birth_date, sex
from bank.client_
left join
  (select client_id,
    str_to_date(concat('19',first2,lpad(second2,2,'0'),last2), '%Y%m%d') as birth_date,
    sex
    from (select client_id,
            first2,
            if(second2 > 50, second2-50, second2) as second2,
            last2,
            if(second2 > 50, 'F', 'M') as sex
            from (select client_id,
                substring(birth_number, 2,2) as first2,
                substring(birth_number, 4,2) as second2,
                substring(birth_number, 6,2) as last2
                from bank.client_) as temp) as temp2) temp
on client_.client_id = temp.client_id;

## Join clinet_temp with disp to form client_disp table
drop table if exists bank.client_disp;
create table bank.client_disp as
select account_id, disp.client_id, district_id, type_, birth_date, sex
  from bank.disp
  left join bank.clients
  on disp.client_id = clients.client_id;

##  Create client_account table
drop table if exists bank.client_account;
create table bank.client_account as
    select
        account_id,
        client_id,
        district_id as client_district_id,
        birth_date,
        sex,
        n_clients
        user_client_id,
        user_district_id,
        user_bday,
        user_sex
    from(select
          account_id,client_id,district_id,birth_date,sex
      from bank.client_disp
      where type_= 'OWNER') o
      left join
            (select account_id as user_account_id,
                client_id as user_client_id,
                district_id as user_district_id,
                birth_date as user_bday,
                sex as user_sex
            from bank.client_disp
            where type_ = 'USER') u
      on o.account_id = u.user_account_id
      left join
          (select account_id,
                count(*) as n_clients
            from bank.client_disp
            group by account_id) c
      using (account_id);

## Drop temp tables
drop table if exists bank.client_;
drop table if exists bank.client_disp;
##### END CLIENTS ######

## ORDERS TABLE
drop table if exists bank.orders;
create table bank.orders(
        order_id int,
        account_id int,
        bank_to char(5),
        account_to varchar(10),
        amount float,
        K_symbol varchar(12));
load data local infile
'/bank_data_loan_default/order.asc'
into table bank.orders
character set UTF8
fields terminated by ';'
lines terminated by '\n'
ignore 1 lines;
update bank.orders
set K_symbol =
        replace(replace(replace(replace(replace(
        K_symbol, '"POJISTNE"', 'insurance')
                ,  '"SIPO"', 'housing')
                ,  '"LEASING"','leasing')
                ,  '"UVER"', 'loan')
                ,  '\r', '');

## orders_account table has one row per account and aggregates order amounts
# overall and by type (housing, loan, insurance, lease)
drop table if exists bank.orders_account;
create table bank.orders_account as
  (select account_id,
      count(*) as n_orders,
      avg(amount) as avg_orders_amount,
      stddev(amount) as std_orders_amount,
      sum(if(K_symbol='housing',1,0)) as n_orders_housing,
      avg(if(K_symbol='housing',amount, NULL)) as avg_orders_housing,
      stddev(if(K_symbol='housing',amount, NULL)) as std_orders_housing,
      sum(if(K_symbol='loan',1,0)) as n_orders_loan,
      avg(if(K_symbol='loan',amount, NULL)) as avg_orders_loan,
      stddev(if(K_symbol='loan',amount, NULL)) as std_orders_loan,
      sum(if(K_symbol='insurance',1,0)) as n_orders_insur,
      avg(if(K_symbol='insurance',amount, NULL)) as avg_orders_insur,
      stddev(if(K_symbol='insurance',amount, NULL)) as std_orders_insur,
      sum(if(K_symbol='leasing',1,0)) as n_orders_leas,
      avg(if(K_symbol='leasing',amount, NULL)) as avg_orders_leas,
      stddev(if(K_symbol='leasing',amount, NULL)) as std_orders_leas
  from bank.orders
  group by account_id);

## TRANSACTIONS
drop table if exists bank.trans;
create table bank.trans(
        trans_id int,
        account_id int,
        date_ date,
        type_ varchar(16),
        operation varchar(17),
        amount decimal(15,2),
        balance varchar(15),
        k_symbol varchar(15),
        bank char(4),
        account_ char(10));
load data local infile
'/bank_data_loan_default/trans.asc'
into table bank.trans
character set UTF8
fields terminated by ';'
lines terminated by '\n'
ignore 1 lines
(trans_id, account_id, @date_, type_, operation, amount, balance, k_symbol, bank, account_)
set date_ = str_to_date(@date_, '%y%m%d');
update bank.trans set
    type_ = replace(replace(replace(
                type_, '"PRIJEM"', 'credit')
                        ,  '"VYDAJ"', 'withdrawl')
                        ,  '"VYBER"', 'withdrawl'),
    operation = replace(replace(replace(replace(replace(
                operation, '"VYBER KARTOU"', 'card_withdrawl')
                                , '"VKLAD"', 'cash_credit')
                                , '"PREVOD Z UCTU"', 'bank_collection')
                                , '"VYBER"', 'cash_withdrawl')
                                , '"PREVOD NA UCET"', 'bank_remittance'),
    account_ = replace(replace(
                account_, '\r', '')
                            ,  '0', ''),
    K_symbol = replace(replace(replace(replace(replace(replace(replace(replace(
                K_symbol, '"POJISTNE"', 'insurance')
                        , '"SLUZBY"', 'statement')
                        , '"UROK"', 'intrestcredit')
                        , '"SANKC. UROK"', 'intrest_nbal')
                        ,  '"SIPO"', 'housing')
                        ,  '"DUCHOD"', 'oldage_pension')
                        ,  '"UVER"', 'loan')
                        , '"', '');

# create trans_before table: holds only transactions from before loan.date_
drop table if exists bank.trans_before;
create table bank.trans_before
select account_id, trans_id, date_, type_, operation, amount,
  balance, k_symbol, bank
from
  (select account_id, trans_id, trans.date_, type_,
    operation, amount, balance, k_symbol, bank,
    coalesce(l.date_, date('1999-12-31')) as date_loan
  from bank.trans
  left join
    (select account_id, date_ from bank.loans) l
  using(account_id)) t
where date_ < date_loan;


# trans_account table
drop table if exists bank.trans_account;
create table bank.trans_account
select account_id,
      count(*) as n_trans,
      sum(if(type_='credit',1,0)) as n_credit_trans,
      sum(if(type_='credit',amount,0)) as sum_credit_trans,
      avg(if(type_='credit',amount,null)) as avg_credit_trans,
      stddev(if(type_='credit',amount,null)) as std_credit_trans,
      sum(if(type_='withdrawl',1,0)) as n_withdrawl_trans,
      sum(if(type_='withdrawl',amount,0)) as sum_withdrawl_trans,
      avg(if(type_='withdrawl',amount,null)) as avg_withdrawl_trans,
      stddev(if(type_='withdrawl',amount,null)) as std_withdrawl_trans,
      sum(if(operation='cash_credit',1,0)) as n_cash_deposit,
      sum(if(operation='bank_collection',1,0)) as n_bank_collection,
      sum(if(operation='cash_withdrawl',1,0)) as n_cash_withdrawl,
      sum(if(operation='bank_remittance',1,0)) as n_bank_remit,
      sum(if(operation='card_withdrawl',1,0)) as n_card_withdrawl,
      sum(if(type_='credit',amount,null)) as total_credits_amount,
      sum(if(type_='withdrawl',amount,null)) as total_withdrawls_amount,
      avg(balance) as avg_balance,
      stddev(balance) as std_balance,
      min(balance) as min_balance,
      max(balance) as max_balance,
      sum(if(balance < 0,1,0)) as n_neg_balance
from bank.trans
group by account_id;

# trans_before_account table
drop table if exists bank.trans_before_account;
create table bank.trans_before_account
select account_id,
      count(*) as n_trans,
      sum(if(type_='credit',1,0)) as n_credit_trans,
      sum(if(type_='credit',amount,0)) as sum_credit_trans,
      avg(if(type_='credit',amount,null)) as avg_credit_trans,
      stddev(if(type_='credit',amount,null)) as std_credit_trans,
      sum(if(type_='withdrawl',1,0)) as n_withdrawl_trans,
      sum(if(type_='withdrawl',amount,0)) as sum_withdrawl_trans,
      avg(if(type_='withdrawl',amount,null)) as avg_withdrawl_trans,
      stddev(if(type_='withdrawl',amount,null)) as std_withdrawl_trans,
      sum(if(operation='cash_credit',1,0)) as n_cash_deposit,
      sum(if(operation='bank_collection',1,0)) as n_bank_collection,
      sum(if(operation='cash_withdrawl',1,0)) as n_cash_withdrawl,
      sum(if(operation='bank_remittance',1,0)) as n_bank_remit,
      sum(if(operation='card_withdrawl',1,0)) as n_card_withdrawl,
      sum(if(type_='credit',amount,null)) as total_credits_amount,
      sum(if(type_='withdrawl',amount,null)) as total_withdrawls_amount,
      avg(balance) as avg_balance,
      stddev(balance) as std_balance,
      min(balance) as min_balance,
      max(balance) as max_balance,
      sum(if(balance < 0,1,0)) as n_neg_balance
from bank.trans_before
group by account_id;


## CARD
drop table if exists bank.card;
create table bank.card(
        card_id int,
        disp_id int,
        type_ varchar(10),
        issued date);
load data local infile
'/bank_data_loan_default/card.asc'
into table bank.card
character set UTF8
fields terminated by ';'
lines terminated by '\n'
ignore 1 lines
(card_id, disp_id, type_, @issued)
set issued = str_to_date(@issued, '%y%m%d %H:%i:%s');
drop table if exists bank.cards_account;
create table bank.cards_account
select account_id,
  count(distinct(card_id)) as n_cards,
  sum(if(card.type_='"gold"',1,0)) as n_cards_gold,
  sum(if(card.type_='"classic"',1,0)) as n_cards_classic,
  sum(if(card.type_='"junior"',1,0)) as n_cards_junior
from bank.card
left join bank.disp
using(disp_id)
group by account_id, card.type_;


## Only Cards before loan
drop table if exists bank.card_before;
create table bank.card_before
select card_id, card.type_, issued, disp_id
from bank.card
left join
  (select account_id, disp_id, client_id, type_,
     coalesce(l.date_, date('1999-12-31')) as date_loan
  from bank.disp
  left join (select account_id, date_ from bank.loans) l
  using(account_id)) ll
using(disp_id)
where issued < date_loan;

drop table if exists bank.cards_before_account;
create table bank.cards_before_account
select account_id,
  count(distinct(card_id)) as n_cards,
  sum(if(card_before.type_='"gold"',1,0)) as n_cards_gold,
  sum(if(card_before.type_='"classic"',1,0)) as n_cards_classic,
  sum(if(card_before.type_='"junior"',1,0)) as n_cards_junior
from bank.card_before
left join bank.disp
using(disp_id)
group by account_id, card_before.type_;
select * from bank.cards_before_account;


## DEMOGRAPHICS
drop table if exists bank.demo;
create table bank.demo(
        district_id int,
        district_name char(25),
        region char(20),
        n_inhab int,
        n_munic_less_50 int,
        n_munic_2000 int,
        n_munic_10000 int,
        n_munic_over int,
        n_cities int,
        urban_ratio float(4,1),
        salary int,
        unemploy_95 float(5,2),
        unemploy_96 float(4,2),
        n_entre_p1000 int,
        n_crimes_95 int,
        n_crimes_96 int
        );
load data local infile
'/bank_data_loan_default/district.asc'
into table bank.demo
character set UTF8
fields terminated by ';'
lines terminated by '\n'
ignore 1 lines;


##### LOANS LEFT JOIN ML ######
drop table if exists bank.ML;
create table bank.ML as select
  account_id,
  loan_id,
  client_id,
  accounts.district_id,
  loans.date_ as date_loan,
  amount as amount_loan,
  duration as duration_loan,
  payments as payments_loan,
  status_ as status_loan,
  current as current_loan,
  good_status as good_status_loan,
  accounts.frequency as freq_dep_acc,
  accounts.date_ as date_account,
  client_district_id,
  birth_date as bday_client,
  sex as sex_client,
  user_client_id,
  user_district_id,
  user_bday,
  user_sex,
  n_orders,
  avg_orders_amount,
  std_orders_amount,
  n_orders_housing,
  avg_orders_housing,
  std_orders_housing,
  n_orders_loan,
  avg_orders_loan,
  std_orders_loan,
  n_orders_insur,
  avg_orders_insur,
  std_orders_insur,
  n_orders_leas,
  avg_orders_leas,
  std_orders_leas,
  n_trans,
  n_credit_trans,
  sum_credit_trans,
  avg_credit_trans,
  std_credit_trans,
  n_withdrawl_trans,
  sum_withdrawl_trans,
  avg_withdrawl_trans,
  std_withdrawl_trans,
  n_cash_deposit,
  n_bank_collection,
  n_cash_withdrawl,
  n_bank_remit,
  n_card_withdrawl,
  total_credits_amount,
  total_withdrawls_amount,
  avg_balance,
  std_balance,
  min_balance,
  max_balance,
  n_neg_balance,
  n_cards,
  n_cards_gold,
  n_cards_classic,
  n_cards_junior,
  demo.district_id as district_id_demo,
  district_name,
  region,
  n_inhab,
  n_munic_less_50,
  n_munic_2000,
  n_munic_10000,
  n_munic_over,
  n_cities,
  urban_ratio,
  salary,
  unemploy_95,
  unemploy_96,
  n_entre_p1000,
  n_crimes_95,
  n_crimes_96
from bank.loans
left join bank.accounts using(account_id)
left join bank.client_account using(account_id)
left join bank.orders_account using(account_id)
left join bank.trans_account using(account_id)
left join bank.cards_account using (account_id)
left join bank.demo on demo.district_id = accounts.district_id
order by account_id;
select * from bank.ML;

select datediff(date_loan,bday_client)/365
from bank.ML;


##### ALL ACCOUNTS: INCLUDES DATA FOR PRE-APPROVING LOANS ######
drop table if exists bank.allML;
create table bank.allML as select
  account_id,
  loan_id,
  client_id,
  accounts.district_id,
  loans.date_ as date_loan,
  amount as amount_loan,
  duration as duration_loan,
  payments as payments_loan,
  status_ as status_loan,
  current as current_loan,
  good_status as good_status_loan,
  accounts.frequency as freq_dep_acc,
  accounts.date_ as date_account,
  datediff(loans.date_,accounts.date_) as days_with_bank,
  client_district_id,
  birth_date as bday_client,
  datediff(loans.date_,birth_date)/365 as age_at_loan,
  sex as sex_client,
  user_client_id,
  user_district_id,
  user_bday,
  user_sex,
  n_orders,
  avg_orders_amount,
  std_orders_amount,
  n_orders_housing,
  avg_orders_housing,
  std_orders_housing,
  n_orders_loan,
  avg_orders_loan,
  std_orders_loan,
  n_orders_insur,
  avg_orders_insur,
  std_orders_insur,
  n_orders_leas,
  avg_orders_leas,
  std_orders_leas,
  n_trans,
  n_credit_trans,
  sum_credit_trans,
  avg_credit_trans,
  std_credit_trans,
  n_withdrawl_trans,
  sum_withdrawl_trans,
  avg_withdrawl_trans,
  std_withdrawl_trans,
  n_cash_deposit,
  n_bank_collection,
  n_cash_withdrawl,
  n_bank_remit,
  n_card_withdrawl,
  total_credits_amount,
  total_withdrawls_amount,
  avg_balance,
  std_balance,
  min_balance,
  max_balance,
  n_neg_balance,
  n_cards,
  n_cards_gold,
  n_cards_classic,
  n_cards_junior,
  demo.district_id as district_id_demo,
  district_name,
  region,
  n_inhab,
  n_munic_less_50,
  n_munic_2000,
  n_munic_10000,
  n_munic_over,
  n_cities,
  urban_ratio,
  salary,
  unemploy_95,
  unemploy_96,
  n_entre_p1000,
  n_crimes_95,
  n_crimes_96
from bank.accounts
left join bank.loans using(account_id)
left join bank.client_account using(account_id)
left join bank.orders_account using(account_id)
left join bank.trans_account using(account_id)
left join bank.cards_account using (account_id)
left join bank.demo on demo.district_id = accounts.district_id
order by account_id;
select * from bank.allML;


#### JOINS FOR ALL ACCOUNTS : ONLY DATA FROM BEFORE LOANS WERE ISSUED
drop table if exists bank.allbeforeML;
create table bank.allbeforeML select
  account_id,
  loan_id,
  client_id,
  accounts.district_id,
  loans.date_ as date_loan,
  amount as amount_loan,
  duration as duration_loan,
  payments as payments_loan,
  status_ as status_loan,
  current as current_loan,
  good_status as good_status_loan,
  accounts.frequency as freq_dep_acc,
  accounts.date_ as date_account,
  datediff(loans.date_,accounts.date_) as days_with_bank,
  client_district_id,
  birth_date as bday_client,
  datediff(loans.date_,birth_date)/365 as age_at_loan,
  sex as sex_client,
  user_client_id,
  user_district_id,
  user_bday,
  user_sex,
  n_orders,
  avg_orders_amount,
  std_orders_amount,
  n_orders_housing,
  avg_orders_housing,
  std_orders_housing,
  n_orders_loan,
  avg_orders_loan,
  std_orders_loan,
  n_orders_insur,
  avg_orders_insur,
  std_orders_insur,
  n_orders_leas,
  avg_orders_leas,
  std_orders_leas,
  n_trans,
  n_credit_trans,
  sum_credit_trans,
  avg_credit_trans,
  std_credit_trans,
  n_withdrawl_trans,
  sum_withdrawl_trans,
  avg_withdrawl_trans,
  std_withdrawl_trans,
  n_cash_deposit,
  n_bank_collection,
  n_cash_withdrawl,
  n_bank_remit,
  n_card_withdrawl,
  total_credits_amount,
  total_withdrawls_amount,
  avg_balance,
  std_balance,
  min_balance,
  max_balance,
  n_neg_balance,
  n_cards,
  n_cards_gold,
  n_cards_classic,
  n_cards_junior,
  demo.district_id as district_id_demo,
  district_name,
  region,
  n_inhab,
  n_munic_less_50,
  n_munic_2000,
  n_munic_10000,
  n_munic_over,
  n_cities,
  urban_ratio,
  salary,
  unemploy_95,
  unemploy_96,
  n_entre_p1000,
  n_crimes_95,
  n_crimes_96
from bank.accounts
left join bank.loans using(account_id)
left join bank.client_account using(account_id)
left join bank.orders_account using(account_id)
left join bank.trans_before_account using(account_id)
left join bank.cards_before_account using (account_id)
left join bank.demo on demo.district_id = accounts.district_id
order by account_id;


# check if client district id is same as account district id
select
    sum(if(a_dist_id=c_dist_id, 1, 0)) as match_,
    sum(if(a_dist_id=c_dist_id, 0, 1)) as mismatch
from
    (select bank.accounts.account_id,
          bank.accounts.district_id as a_dist_id,
          bank.client_account.client_district_id as c_dist_id
    from bank.accounts
    left join bank.client_account
    using (account_id))a;




