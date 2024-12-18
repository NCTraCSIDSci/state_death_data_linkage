-- Databricks notebook source

/*
This file contains some additional code used in the development of the code in death_data_linkage_prod.sql, 
including extracting some data for training (which was then manually labeled), 
training the ML model used in the prod code, and evaluating model performance on the training data using cross-validation.

Authors:
Peter J. Leese and John P. Powers

Copyright and licensing:
© 2024, The University of North Carolina at Chapel Hill. Permission is granted to use in accordance with the MIT license. 

The code is licensed under the open-source MIT license.
*/

-- MAGIC %md
-- MAGIC ## Data Loading



-- COMMAND ----------

-- see death_data_linkage_prod.sql for creation of total_linkage_results table


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Training Data Supplement

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Creating a supplement for the training dataset focused on the difficult classification range of 30 <= wms <= 50

-- COMMAND ----------

DROP VIEW IF EXISTS train_data_supp;

CREATE TEMPORARY VIEW train_data_supp AS
SELECT *
FROM total_linkage_results
WHERE wms BETWEEN 30 AND 50
LIMIT 200

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC train_data_supp = spark.sql('SELECT * FROM train_data_supp')

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC def write_csv(df_for_csv, file_path):
-- MAGIC   file_dir = file_path[:-5] + '_dir'
-- MAGIC   df_for_csv.coalesce(1).write.option('header', 'true').csv(file_dir)
-- MAGIC   dbutils.fs.mv(dbutils.fs.ls(file_dir)[3].path, file_path)
-- MAGIC   dbutils.fs.rm(file_dir, recurse=True)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC write_csv(train_data_supp, '[path].csv')



-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## ML Classifier

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Load Code and Training Data

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC # import code
-- MAGIC import pandas as pd
-- MAGIC import numpy as np
-- MAGIC pd.plotting.register_matplotlib_converters()
-- MAGIC import matplotlib.pyplot as plt
-- MAGIC %matplotlib inline
-- MAGIC import seaborn as sns
-- MAGIC import joblib
-- MAGIC
-- MAGIC # PySpark
-- MAGIC import pyspark.sql.functions as F
-- MAGIC from pyspark.sql.types import IntegerType
-- MAGIC from pyspark.sql import Window
-- MAGIC
-- MAGIC # class weights
-- MAGIC from sklearn.utils.class_weight import compute_sample_weight
-- MAGIC
-- MAGIC # cross validation and modeling
-- MAGIC from sklearn.pipeline import make_pipeline
-- MAGIC from sklearn.model_selection import cross_validate
-- MAGIC from sklearn.model_selection import StratifiedKFold
-- MAGIC from sklearn.ensemble import HistGradientBoostingClassifier
-- MAGIC
-- MAGIC # tuning
-- MAGIC from sklearn.model_selection import GridSearchCV  
-- MAGIC from sklearn.model_selection import validation_curve
-- MAGIC
-- MAGIC # performance
-- MAGIC from sklearn.model_selection import cross_val_predict
-- MAGIC from sklearn.metrics import confusion_matrix
-- MAGIC from sklearn.metrics import ConfusionMatrixDisplay
-- MAGIC
-- MAGIC # feature importance
-- MAGIC from sklearn.model_selection import train_test_split
-- MAGIC from sklearn.inspection import permutation_importance

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC # load ML training data
-- MAGIC train_data_1 = spark.read.format("csv").option("header","true").load("[path].csv")
-- MAGIC train_data_2 = spark.read.format("csv").option("header","true").load("[path].csv")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Training Data Preprocessing

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC # combine into full training dataset
-- MAGIC train_data = pd.concat([train_data_1, train_data_2])
-- MAGIC train_data = train_data[['likely_match', 'wms', 'lname_dist', 'fname_dist', 'dob_dist', 'ssn_dist', 'zip_dist', 'house_dist', 
-- MAGIC                           'lname_score', 'fname_score', 'mname_score', 'dob_score', 'ssn_score', 'zip_score', 'house_score']]
-- MAGIC train_data[train_data.columns] = train_data[train_data.columns].apply(pd.to_numeric)
-- MAGIC train_data.info()

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Model Training

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC # define target
-- MAGIC target = train_data['likely_match']
-- MAGIC
-- MAGIC # define features
-- MAGIC features = train_data[['lname_dist', 'fname_dist', 'dob_dist', 'ssn_dist', 'zip_dist', 'house_dist', 
-- MAGIC                   'lname_score', 'fname_score', 'mname_score', 'dob_score', 'ssn_score', 'zip_score', 
-- MAGIC                   'house_score']].copy()
-- MAGIC
-- MAGIC # define cross validation procedure
-- MAGIC cross_val = StratifiedKFold(n_splits=5)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC # model setup
-- MAGIC
-- MAGIC # have to manually adjust weights to correct for class imbalance
-- MAGIC # since HistGradientBoostingClassifier doesn't have a built-in class_weight parameter
-- MAGIC sample_weights = compute_sample_weight(class_weight='balanced', y=target)
-- MAGIC
-- MAGIC # define algorithm
-- MAGIC hgbc = HistGradientBoostingClassifier(random_state=0, learning_rate=0.1)
-- MAGIC
-- MAGIC # assemble steps into pipeline
-- MAGIC pipeline = make_pipeline(hgbc)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC # hyperparameter tuning
-- MAGIC
-- MAGIC # hyperparameter values to try
-- MAGIC param_grid =  {
-- MAGIC     'histgradientboostingclassifier__max_iter': [50, 100, 150, 200, 250],
-- MAGIC     'histgradientboostingclassifier__max_leaf_nodes': [8, 16, 32, 64, 128, 256]}
-- MAGIC
-- MAGIC grid_search = GridSearchCV(pipeline, param_grid=param_grid, scoring='roc_auc', n_jobs=-1)
-- MAGIC
-- MAGIC cv_results = cross_validate(grid_search, features, target, n_jobs=-1, 
-- MAGIC                             fit_params={'histgradientboostingclassifier__sample_weight': sample_weights}, 
-- MAGIC                             return_estimator=True)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC # get best values of hyperparameters for each split of outer cross-validation
-- MAGIC for cv_split, estimator_in_split in enumerate(cv_results["estimator"]):
-- MAGIC     print(
-- MAGIC         f"Best hyperparameters for split #{cv_split + 1}:\n"
-- MAGIC         f"{estimator_in_split.best_params_}\n"
-- MAGIC         f"with AUROC of {cv_results['test_score'][cv_split]:.3f}"
-- MAGIC     )

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC pipeline.set_params(histgradientboostingclassifier__max_iter=150)
-- MAGIC
-- MAGIC # generate validation curve for max_leaf_nodes
-- MAGIC max_leaf_nodes_values = [4, 8, 12, 16, 20, 24]
-- MAGIC train_scores, test_scores = validation_curve(
-- MAGIC     pipeline, features, target, param_name='histgradientboostingclassifier__max_leaf_nodes', 
-- MAGIC     param_range=max_leaf_nodes_values, scoring='roc_auc', n_jobs=-1)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC # plot validation curve for max_leaf_nodes
-- MAGIC plt.errorbar(max_leaf_nodes_values, train_scores.mean(axis=1), yerr=train_scores.std(axis=1), label="Training score")
-- MAGIC plt.errorbar(max_leaf_nodes_values, test_scores.mean(axis=1), yerr=test_scores.std(axis=1), label="Testing score")
-- MAGIC plt.legend()
-- MAGIC plt.xticks(np.arange(0, 30, 2))
-- MAGIC plt.xlabel("max_leaf_nodes")
-- MAGIC plt.ylabel("AUROC")
-- MAGIC _ = plt.title("Validation curve for max_leaf_nodes")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC pipeline.set_params(histgradientboostingclassifier__max_iter=150)
-- MAGIC pipeline.set_params(histgradientboostingclassifier__max_leaf_nodes=20)
-- MAGIC
-- MAGIC # get results from tuned model
-- MAGIC hgbc_results = cross_validate(pipeline, features, target, 
-- MAGIC                               scoring=['accuracy', 'precision', 'recall', 'roc_auc'], cv=cross_val, 
-- MAGIC                               fit_params={'histgradientboostingclassifier__sample_weight': sample_weights},
-- MAGIC                               return_estimator=True)

-- COMMAND ----------

-- MAGIC %python 
-- MAGIC
-- MAGIC pipeline.fit(features, target)
-- MAGIC joblib.dump(pipeline, '[path].joblib')

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Model Performance

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC # note these metrics reflect performance before additional wms threshold
-- MAGIC print('The mean accuracy with cross-validation is: ' 
-- MAGIC       f'{hgbc_results["test_accuracy"].mean():.3f} +/- {hgbc_results["test_accuracy"].std():.3f}') 
-- MAGIC print('The mean precision with cross-validation is: ' 
-- MAGIC       f'{hgbc_results["test_precision"].mean():.3f} +/- {hgbc_results["test_precision"].std():.3f}') 
-- MAGIC print('The mean recall with cross-validation is: ' 
-- MAGIC       f'{hgbc_results["test_recall"].mean():.3f} +/- {hgbc_results["test_recall"].std():.3f}') 
-- MAGIC print('The mean AUROC with cross-validation is: ' 
-- MAGIC       f'{hgbc_results["test_roc_auc"].mean():.3f} +/- {hgbc_results["test_roc_auc"].std():.3f}') 

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC # confusion matrix for model on full training dataset
-- MAGIC hgbc_preds = cross_val_predict(pipeline, features, target, 
-- MAGIC                               cv=cross_val, 
-- MAGIC                               fit_params={'histgradientboostingclassifier__sample_weight': sample_weights})
-- MAGIC hgbc_preds[train_data['wms'] < 30] = 0
-- MAGIC conf_mat = confusion_matrix(target, hgbc_preds)
-- MAGIC disp = ConfusionMatrixDisplay(conf_mat)
-- MAGIC disp.plot()
-- MAGIC plt.show()

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC # examine mismatches between manual review and model results
-- MAGIC train_data['pred'] = hgbc_preds
-- MAGIC train_data[train_data['likely_match'] != train_data['pred']]

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC # compute permutation feature importances
-- MAGIC
-- MAGIC # create dataframe to store results
-- MAGIC perm_results_df = pd.DataFrame(index=features.columns, columns=range(25), dtype='float64')
-- MAGIC
-- MAGIC # define randomization control for train_test_split for reproducibility
-- MAGIC rng = np.random.RandomState(0)
-- MAGIC
-- MAGIC for i in range(25):
-- MAGIC
-- MAGIC     # split data for permutation importance computation
-- MAGIC     X_train, X_test, y_train, y_test = train_test_split(features, target, test_size=0.2, random_state=rng)
-- MAGIC
-- MAGIC     # permutation importance computation
-- MAGIC     pipeline.fit(X_train, y_train)
-- MAGIC     perm_results = permutation_importance(pipeline, X_test, y_test, scoring='roc_auc', 
-- MAGIC                                           n_repeats=10, random_state=0, n_jobs=-1)
-- MAGIC
-- MAGIC     # store results in a dataframe
-- MAGIC     perm_results_df.iloc[:, i] = perm_results.importances_mean
-- MAGIC     
-- MAGIC # summarize results over iterations and sort
-- MAGIC perm_results_summary = pd.DataFrame(index=features.columns)
-- MAGIC perm_results_summary['mean'] = perm_results_df.mean(axis=1)
-- MAGIC perm_results_summary['std'] = perm_results_df.std(axis=1)
-- MAGIC perm_results_summary.sort_values('mean', ascending=False, inplace=True)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC # view results
-- MAGIC print(perm_results_summary)
-- MAGIC plt.title('Permutation Importances')
-- MAGIC sns.barplot(x=perm_results_summary['mean'], y=perm_results_summary.index, xerr=perm_results_summary['std'])
-- MAGIC
-- MAGIC None
