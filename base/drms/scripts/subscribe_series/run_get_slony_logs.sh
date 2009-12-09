#source ~/.ssh-agent_rs
export SSH_AUTH_SOCK=/tmp/ssh-HgUgDH2809/agent.2809;
export SSH_AGENT_PID=2810;
echo "SSH_AGENT_PID=[$SSH_AGENT_PID]"
date

./get_slony_logs.pl jsoc mydb

