# Cron Schedule

All times UTC. Add to the app-user's crontab on the production server (`crontab -e`).

```
# Scrapers — 2 AM daily
0 2 * * *      cd /opt/forced-action && python -m src.tasks.run_scrapers hillsborough

# CDS Scoring — 7 AM daily (after scrapers)
0 7 * * *      cd /opt/forced-action && python -m src.services.cds_engine --rescore-all

# Subscriber lead emails — 10 AM UTC weekdays (6 AM EDT / after scoring)
0 10 * * 1-5   cd /opt/forced-action && python -m src.tasks.subscriber_email

# Match rate monitor — 9 AM daily
0 9 * * *      cd /opt/forced-action && python -m src.tasks.match_rate_monitor

# Unmatched record rematch — Sunday 3:30 AM UTC (before Monday scrapers)
30 3 * * 0     cd /opt/forced-action && python -m src.tasks.rematch_unmatched

# DB backup — daily 1 AM UTC (before scrapers), weekly on Sunday
0 1 * * *      cd /opt/forced-action && python scripts/backup_db.py          >> /var/log/fa-backup.log 2>&1
0 1 * * 0      cd /opt/forced-action && python scripts/backup_db.py --weekly >> /var/log/fa-backup.log 2>&1
```
