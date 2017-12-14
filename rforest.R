
library(glmnet)

ddir = "D:/dev/kaggle/wsdm/train"
fpath = file.path(ddir, "df_norm.csv")

df = read.csv(fpath)
head(df)

xcols <- colnames(df)[2:length(colnames(df))]
ycols = colnames(df)[1]

x = as.matrix(df[xcols])
y = as.matrix(df[ycols])
head(y)

cvfit = cv.glmnet(x, y, family="binomial")
plot(cvfit)
coef(cvfit, s="lambda.1se")


predy = predict(cvfit, x, s="lambda.min", 
                type="response")
summary(predy)

N = length(y)
score = -(1.0/N)*sum(y * log(predy) + 
                    (1-y)*log(1-predy))
score


ddir = "D:/dev/kaggle/wsdm/test"
fpath = file.path(ddir, "df_norm.csv")
