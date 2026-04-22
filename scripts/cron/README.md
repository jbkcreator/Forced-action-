# Cron Schedule

All times UTC. Add to the app-user's crontab on the production server (`crontab -e`).

```
# Individual scrapers are scheduled in scripts/cron/crontab.txt (see that file
# for the canonical daily schedule). The removed run_scrapers.py orchestrator
# used to run all scrapers at 2 AM — that role is now served by the individual
# cron lines in crontab.txt.

# CDS Scoring — 7 AM daily (after scrapers)
0 7 * * *      cd /opt/forced-action && python -m src.services.cds_engine --rescore-all

# Subscriber lead emails — 10 AM UTC weekdays (6 AM EDT / after scoring)
0 10 * * 1-5   cd /opt/forced-action && python -m src.tasks.subscriber_email

# Match rate monitor — 9 AM daily
0 9 * * *      cd /opt/forced-action && python -m src.tasks.match_rate_monitor

# Ops health check — 9:30 AM daily (after scoring + match rate monitor)
30 9 * * *     cd /opt/forced-action && python -m src.tasks.health_check      >> /var/log/fa-health.log 2>&1

# Unmatched record rematch — Sunday 3:30 AM UTC (before Monday scrapers)
30 3 * * 0     cd /opt/forced-action && python -m src.tasks.rematch_unmatched

# Daily ops report + stakeholder email — 8 AM UTC daily (after scoring completes at 7 AM)
0 8 * * *      cd /opt/forced-action && python -m src.tasks.daily_report     >> /var/log/fa-daily-report.log 2>&1

# Weekly ops report + stakeholder email — Monday 9 AM UTC (covers Mon–Fri of prior week)
0 9 * * 1      cd /opt/forced-action && python -m src.tasks.weekly_report    >> /var/log/fa-weekly-report.log 2>&1

# Load validator — 8:30 AM daily (after scoring; alerts on zero-record or anomalous scrapers)
30 8 * * *     cd /opt/forced-action && python -m src.tasks.load_validator   >> /var/log/fa-load-validator.log 2>&1

# DB backup — daily 1 AM UTC (before scrapers), weekly on Sunday
0 1 * * *      cd /opt/forced-action && python scripts/backup_db.py          >> /var/log/fa-backup.log 2>&1
0 1 * * 0      cd /opt/forced-action && python scripts/backup_db.py --weekly >> /var/log/fa-backup.log 2>&1
```
