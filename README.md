```
To update the nymx-mail and received folder with a 1970-01-01-00:00 timestamp every 5 minutes
paste the following lines, when logged in your $HOME/nymx account, in your terminal, as cronjob:

(crontab -l 2>/dev/null; echo "*/5 * * * * TZ=UTC find ~/nymx-mail ~/received -print0 | xargs -0 touch -t 197001010000.00") | crontab -
```
