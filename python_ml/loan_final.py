
# coding: utf-8

# In[1]:

import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn import metrics
from sklearn.linear_model import LogisticRegression
from sklearn.model_selection import GridSearchCV
from sklearn import tree
import matplotlib.pyplot as plt
from imblearn.over_sampling import RandomOverSampler
from sklearn.ensemble import BaggingClassifier
import configparser # For hiding passwords.


# In[2]:

## CONNECT TO SQL

get_ipython().magic('load_ext sql')
get_ipython().magic('config SqlMagic.autopandas = True')

# configure database 
config = configparser.ConfigParser()
config.read('databases.ini')
logins = config['bank']
dbconnect = 'mysql://{}:{}@{}/{}'.format(
    logins['user'], logins['password'], 
    logins['host'], logins['database'])

get_ipython().magic('sql $dbconnect')
# %sql set sql_safe_updates = 0;


# In[3]:

## GET ALL ACCOUNTS
accounts = get_ipython().magic('sql select * from bank.allbeforeML')
accounts.set_index('account_id', inplace=True)


# In[4]:

# PRE PROCESSING: creeat dummies and convert types
print('running preprocessing')
accounts = pd.get_dummies(accounts,
	columns=['freq_dep_acc','sex_client','user_sex'], drop_first=True)

str_to_int_col = ['n_orders_housing','n_orders_loan', 'n_orders_insur', 'n_orders_leas',
              'n_credit_trans','n_withdrawl_trans', 'n_cash_deposit', 'n_bank_collection',
              'n_cash_withdrawl', 'n_bank_remit', 'n_card_withdrawl', 'n_neg_balance',
              'current_loan', 'good_status_loan']

str_to_num_col = ['avg_orders_leas', 'std_orders_leas', 'sum_credit_trans',
                  'avg_credit_trans', 'sum_withdrawl_trans', 'avg_withdrawl_trans',
                  'total_credits_amount', 'total_withdrawls_amount', 'min_balance', 
                  'max_balance', 'n_cards_gold', 'n_cards_classic','n_cards_junior']

accounts[str_to_num_col] = accounts[str_to_num_col].fillna(value=np.nan).astype(float)
accounts[str_to_int_col] = accounts[str_to_int_col].fillna(value=0).astype(int)


# SELECT ONLY ACCOUNTS WITH LOANS : TRAIN & TEST MODEL ON
loans = accounts[~accounts['loan_id'].isnull()]


# CHOOSE FEATURES
cols_X = [ 'n_orders', 'avg_orders_amount',
           'std_orders_amount', 'n_orders_housing', 'avg_orders_housing',
           'std_orders_housing', 'n_orders_insur', 'avg_orders_insur',
           'std_orders_insur', 'n_orders_leas', 'avg_orders_leas',
           'std_orders_leas', 'n_trans', 'n_credit_trans', 'sum_credit_trans',
           'avg_credit_trans', 'std_credit_trans', 'n_withdrawl_trans',
           'sum_withdrawl_trans', 'avg_withdrawl_trans', 'std_withdrawl_trans',
           'n_cash_deposit', 'n_bank_collection', 'n_cash_withdrawl',
           'n_bank_remit', 'n_card_withdrawl', 'total_credits_amount',
           'total_withdrawls_amount', 'avg_balance', 'std_balance', 'min_balance',
           'max_balance', 'n_neg_balance', 'n_cards', 'n_cards_gold',
           'n_cards_classic', 'n_cards_junior','days_with_bank', 'age_at_loan', 
           'freq_dep_acc_monthly','freq_dep_acc_weekly']


# SELECT ONLY ACCOUNTS WITHOUT LOANS: FOR PREAPPROVALS
pool = accounts[accounts['loan_id'].isnull()][cols_X].fillna(0)

# SELECT X AND Y
X = loans[cols_X].fillna(0)
y = loans['good_status_loan']


# PREPARE FOR TRAINING AND TESTING
X_train, X_test, y_train, y_test = train_test_split(X,y,random_state = 1)
clf = LogisticRegression()
param_grid = {'C':[1e-5, 3e-5, 1e-4, 3e-4, 1e-3, 3e-3, .01, .03, .1, .3, 1, 3, 10]}


# In[5]:

# RUN BENCHMARK RANDOM FOREST
print('Random Forest Results:')
clf = RandomForestClassifier(random_state=400)
clf.fit(X_train,y_train, )
y_hat = clf.predict(X_test)
print(metrics.classification_report(y_hat,y_test))


# In[6]:

good_loans = loans[loans['good_status_loan']==1]
bad_loans = loans[loans['good_status_loan']==0]
                  
plot_features = ['n_neg_balance','min_balance','avg_balance','std_balance','avg_orders_housing']
for column in plot_features:
        print('making figure '+ column)
        X_range = np.arange(min(X_train[column]),max(X_train[column]))
        X_range = np.reshape(a=X_range, newshape=(len(X_range),1))
        plt.figure(num=None, figsize=(7, 4), dpi=80, facecolor='w', edgecolor='k')
        plt.plot(good_loans[column],np.random.normal(loc=1, scale=.1, size=len(good_loans)),'bo', alpha = .2)
        plt.plot(bad_loans[column],np.random.normal(loc=0, scale=.1, size=len(bad_loans)),'ro', alpha = .2)
        #plt.plot(X_range,np.ones(len(X_range))*.975, 'g', alpha = 1, lw=5)
        plt.title(column)
        plt.xlabel(column)
        plt.ylabel('Good Loan')
        plt.savefig(column+'.png')

        #plt.show()


# In[18]:

# PREPARE FOR BAGGING: choose reps, seeds, and initalize datastructures
nreps = 10
seeds = [105 * i for i in range(nreps)]
 
y_hats = pd.DataFrame(0, index=y_test.index, 
	columns=['true_good']+['rep'+str(i) for i in range(1,nreps+1)])
y_hats['true_good'] = y_test

y_hats_all = pd.DataFrame(0, index=y.index, 
	columns=['true_good']+['rep'+str(i) for i in range(1,nreps+1)])

pool_pred = pd.DataFrame(0, index = pool.index,
	columns=['rep'+str(i) for i in range(1,nreps+1)])

clfs = [0 for i in range(nreps)]
clf = LogisticRegression()
# RUN BAGGING 
print('running fit and predict')
for i, rep in enumerate(y_hats.drop('true_good', axis=1)):
    #print('Working on model ' + str(i))

    # Random Over Sample Training Data
    ros = RandomOverSampler(random_state=seeds[i])
    X_res, y_res = ros.fit_sample(X_train,y_train)
    
    # Fit Logistic Regression with optimal C-regularization parameter
    clf_lr = GridSearchCV(clf, param_grid, scoring = 'recall_micro', verbose=10, n_jobs=-1)
    clf_lr.fit(X_res, y_res,)
    
    # Predict probability of being good loan 
    y_hats[rep] = clf_lr.predict_proba(X_test)[:,1]
    y_hats_all[rep] = clf_lr.predict_proba(X)[:,1]
    pool_pred[rep] = clf_lr.predict_proba(pool)[:,1]
        
    # Record clf for this itteration
    clfs[i] = clf_lr

# Get Average
y_avg_prob = y_hats.iloc[:,1:].mean(axis=1)
y_avg_all = y_hats_all.iloc[:,1:].mean(axis=1)
y_hat = [avg < 0.5 for avg in y_avg_prob]


# In[19]:

# Examine Predictions for test set
good_loans = y_avg_all[y == 1]
bad_loans = y_avg_all[y == 0]

plt.plot(good_loans, np.random.normal(1,.1, len(good_loans)), 'o', alpha=.5)
plt.plot(bad_loans, np.random.normal(0,.1, len(bad_loans)), 'ro', alpha = .5)
plt.xlabel('Probability of Being Good')
plt.ylabel('Acutally Good')
#plt.show()


# In[67]:

crit = 0.68
true_good = good_loans[good_loans>=crit]
false_good = bad_loans[bad_loans>=crit]
true_bad = bad_loans[bad_loans<crit]
false_bad = good_loans[good_loans<crit]

plt.plot(true_good, np.random.normal(1,.1, len(true_good)), 'o', alpha=.5)
plt.plot(false_bad, np.random.normal(1,.1, len(false_bad)), 'mo', alpha=.5)
plt.plot(true_bad, np.random.normal(0,.1, len(true_bad)), 'ko', alpha = .5)
plt.plot(false_good, np.random.normal(0,.1, len(false_good)), 'ro', alpha = .5)
plt.plot([crit,crit],[1.5, -.5], 'k')
plt.text(crit-.2,1.3,'crit = .68')
plt.xlabel('Probability of Being Good')
plt.ylabel('Acutally Good')
#plt.show()


# In[20]:

crit_list=[]
fb_list=[]
tb_list=[]

for crit in np.arange(0,1.01,.01):
    FalsePositives = len(bad_loans[bad_loans>crit])
    TruePositives = len(good_loans[good_loans>crit])
    FalsePositives,TruePositives
    
    crit_list.append(crit)
    fb_list.append(FalsePositives/len(bad_loans))
    tb_list.append(TruePositives/len(good_loans))

roc_table = pd.DataFrame({'crit': crit_list, 
                          'FalsePositive': fb_list, 
                          'TruePositive':tb_list})


# In[68]:

choose_crit = .68
plt.plot(roc_table['FalsePositive'], roc_table['TruePositive'], lw=5, color='k')
plt.xlabel('Bad Loans Accepted')
plt.ylabel('Good Loans Accepted')
plt.plot(roc_table[roc_table.crit==choose_crit]['FalsePositive'], 
         roc_table[roc_table.crit==choose_crit]['TruePositive'], 'y*', markeredgewidth=10)
plt.text(roc_table[roc_table.crit==choose_crit]['FalsePositive']+0.06, 
         roc_table[roc_table.crit==choose_crit]['TruePositive']-.03, 'crit = ' + str(choose_crit))
plt.plot(roc_table[roc_table.crit==.9]['FalsePositive'], 
         roc_table[roc_table.crit==.9]['TruePositive'], 'm*', markeredgewidth=10)
plt.text(roc_table[roc_table.crit==.9]['FalsePositive']+0.06, 
         roc_table[roc_table.crit==.9]['TruePositive'], 'crit = ' + str(0.9))
plt.plot(roc_table[roc_table.crit==.5]['FalsePositive'], 
         roc_table[roc_table.crit==.5]['TruePositive'], 'r*', markeredgewidth=10)
plt.text(roc_table[roc_table.crit==.5]['FalsePositive']+0.06, 
         roc_table[roc_table.crit==.5]['TruePositive']-.04, 'crit = ' + str(0.5))
         
plt.axis('square')
plt.xlim([-0.05,1.05])
plt.ylim([-0.05,1.05])
#plt.show()


# ##### y_pred = y_avg_all > choose_crit
# print(metrics.classification_report(y,y_pred))

# In[23]:

pool_avg_pred = pool_pred.iloc[:,1:].mean(axis=1)
best = pool_avg_pred[pool_avg_pred>=.99]
others =  pool_avg_pred[pool_avg_pred<.99]
print('Accounts found with average of 99% probability of being a good loan:', 
	len(best))

plt.hist(pool_avg_pred, alpha = .75, bins=22)
plt.xlabel('Average Probability of Being Good')
plt.ylabel('count')
#plt.show()
#plt.savefig('predict_histogram.png', bbox_inches='tight')


# In[24]:

preapprove_c = pool_avg_pred.quantile(q=.95)
approve_c = .68

pool_avg_pred = pool_pred.iloc[:,1:].mean(axis=1)
pre_approve = pool_avg_pred[pool_avg_pred >= preapprove_c]
approve = pool_avg_pred[(pool_avg_pred >= approve_c) &
                       (pool_avg_pred < preapprove_c)]
deny = pool_avg_pred[pool_avg_pred < approve_c]

plt.hist(pre_approve, bins=2, color='g', alpha = 0.5)
plt.hist(approve, bins = 5, color='b', alpha=0.5)
plt.hist(deny, color='k', alpha=.5, bins =15)
plt.xlabel('Average Probability of Being Good')
plt.ylabel('Count')
#plt.show()




