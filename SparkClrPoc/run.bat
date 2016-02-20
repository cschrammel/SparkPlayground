set SPARK_HOME=C:\apps\sparkclr\tools\spark-1.5.2-bin-hadoop2.6
set path=%SPARK_HOME%;%path%
set HADOOP_HOME=C:\winutils
set SPARKCLR_HOME=C:\apps\sparkclr\runtime

cd C:\github\sparkclr\build\runtime
scripts\sparkclr-submit --total-executor-cores 2 --exe SparkClrPoc.exe C:\github\sparkplayground\SparkClrPoc\bin\Release


scripts\sparkclr-submit debug
