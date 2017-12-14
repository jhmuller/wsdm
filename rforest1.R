
library(randomForest)

tdir = "C:/Users/jmull/dev/kaggle/wsdm/train"
fpath = file.path(tdir,"model_0.csv")
train = read.csv(fpath)


head(train)

fit <- randomForest(is_churn ~ bd + gender + 
                     # as.factor(registered_via) + 
                      months_since_reg + days_since_last_trans + 
                      payment_plan_days + actual_amount_paid +
                    #  as.factor(payment_method_id) +
                     lcount + num_985 + num_unq + total_secs,
                    data=train, 
                    na.action=na.omit,
                    importance=TRUE, 
                    ntree=200)

sum(is.na(train$is_churn))
as.factor(train$registered_via)
