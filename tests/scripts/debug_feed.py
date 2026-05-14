from src.core.database import get_db_context
from src.core.models import Subscriber, ZipTerritory, DistressScore, Property, StripeWebhookEvent
from sqlalchemy import func, text

FEED_UUID = '470835d9-4404-4fe4-a8f7-f67e81e61d40'

with get_db_context() as db:
    sub = db.query(Subscriber).filter_by(event_feed_uuid=FEED_UUID).first()
    if not sub:
        print('ERROR: subscriber not found')
        exit()

    print('=== SUBSCRIBER ===')
    print('id            :', sub.id)
    print('tier          :', sub.tier)
    print('status        :', sub.status)
    print('vertical      :', sub.vertical)
    print('county_id     :', sub.county_id)
    print('stripe_cust   :', sub.stripe_customer_id)
    print('stripe_sub_id :', sub.stripe_subscription_id)

    print('\n=== ZIP TERRITORY ===')
    zips = db.query(ZipTerritory).filter_by(subscriber_id=sub.id).all()
    if zips:
        for z in zips:
            print(f'  {z.zip_code} — {z.status}')
    else:
        print('  NONE — feed will be empty')

    print('\n=== WALLET ===')
    from src.core.models import WalletBalance
    wallet = db.query(WalletBalance).filter_by(subscriber_id=sub.id).first()
    if wallet:
        print('  balance  :', wallet.balance)
        print('  lifetime :', wallet.lifetime_credits)
    else:
        print('  NO WALLET ROW')

    print('\n=== STRIPE WEBHOOK EVENTS ===')
    events = db.query(StripeWebhookEvent).filter(
        StripeWebhookEvent.stripe_event_id.isnot(None)
    ).order_by(StripeWebhookEvent.created_at.desc()).limit(10).all()
    cust_events = [e for e in events if sub.stripe_customer_id in str(e.payload or '')]
    if cust_events:
        for e in cust_events:
            print(f'  {e.event_type} — {e.status} — {e.created_at}')
    else:
        # Show last 5 regardless so we can see if webhooks are firing at all
        recent = db.query(StripeWebhookEvent).order_by(
            StripeWebhookEvent.created_at.desc()
        ).limit(5).all()
        print('  No events for this customer. Last 5 stripe events overall:')
        for e in recent:
            print(f'    {e.event_type} — {e.status} — {e.created_at}')
