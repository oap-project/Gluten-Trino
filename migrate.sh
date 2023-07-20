first_commit=38ed246f28
last_commit=217aae49e2
git checkout $last_commit

commit_msg=`git log $first_commit..$last_commit --pretty=format:"%s" `
git checkout --orphan dev_module
git reset 
git diff --name-only $first_commit $last_commit > diff_files
git add trino-cpp
git add .mvn .gitignore .gitmodules .java-version LICENSE migrate.sh 
mkdir trino-cpp-plugin
files=`cat diff_files|grep java|grep -v trino-cpp|grep -v testing`

# echo $files|cut -d'/' -f3-|awk '{print "rsync -Rav --relative "$0" trino-cpp-plugin/"}'|sh

cat diff_files|grep java|grep -v trino-cpp|grep -v testing| while read -r file; do
  to_path=`echo $file|cut -d'/' -f3-|rev|cut -d'/' -f2-|rev`
  mkdir -p trino-cpp-plugin/$to_path
  cp --path $file trino-cpp-plugin/$to_path
done
#   cp $file trino-cpp-plugin/$to_path

git add trino-cpp-plugin
git clean -f -d
git commit -m "migrate to new branch, previous commit log: " -m "$commit_msg"
