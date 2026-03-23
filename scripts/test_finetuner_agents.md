# Finetuner Agent Test Scripts

Test each agent by pasting YOUR lines into the Finetuner chat test.
The expected agent response is listed after each line.

---

## TEST 1 — Roofing Agent: Happy Path (Sample Leads + Demo)

**Agent speaks first:**
> "Hi, is this the business owner? Great — my name is Alex, I'm calling from Forced Action..."

**You:** Yeah this is Mike, what's up?

> Expected: Agent delivers the HOOK — monitoring Hillsborough County 24/7, insurance claims, fire reports, storm events.

**You:** Okay, interesting. What kind of events?

> Expected: Agent delivers the BRIDGE — "Right now we have 14 events in your area. One had an insurance claim filed 3 days ago..."

**You:** Yeah I'd be interested to see those

> Expected: Agent delivers SAMPLE OFFER — "Want me to text you 3 live opportunities from your ZIP..."

**You:** Sure, send them over

> Expected: "Perfect, I'm sending those right now — you should get a text in about 60 seconds." Then moves to FOUNDING CLOSE.

**You:** How does the pricing work?

> Expected: "Founding rate is $600/month, one contractor per ZIP. Most guys make that back on one job..."

**You:** That's not bad. Can I see a demo first?

> Expected: DEMO CLOSE — "I can get you a quick call with Josh..." and offers scheduling link.

**You:** Yeah send me the link

> Expected: "Sending it now — takes 5 minutes, no commitment." Warm goodbye.

### PASS if: Agent followed Steps 1-5 in order, stayed on script, didn't mention CloudInnovate or project management.

---

## TEST 2 — Roofing Agent: Objection Path (Skeptical Prospect)

**Agent speaks first:**
> Greeting message

**You:** Yeah who is this?

> Expected: Brief intro — Alex from Forced Action, asks for 30 seconds.

**You:** I already get plenty of leads from HomeAdvisor

> Expected: "Totally fair. The difference is timing — these are properties where the event just happened and nobody has called yet. Want to see 3 for free?"

**You:** Sounds like a scam honestly. How'd you get my number?

> Expected: "Fair question — you're in the DBPR roofing contractor registry for Hillsborough. We reach out to licensed contractors..."

**You:** Just send me an email

> Expected: "Happy to, but honestly a text with live leads is faster. Can I send those to this number?"

**You:** No I'm not interested

> Expected: Agent accepts gracefully, doesn't push further.

**You:** I said I'm not interested

> Expected: "No problem at all — I won't call again. Have a great day." Ends call.

### PASS if: Agent handled all 4 objections correctly, didn't get aggressive, ended after 2 rejections.

---

## TEST 3 — Roofing Agent: Quick Rejection

**Agent speaks first:**
> Greeting message

**You:** Not interested, take me off your list

> Expected: "No problem at all — I won't call again. Have a great day." Immediate graceful exit.

### PASS if: Agent didn't try to pitch, ended immediately and politely.

---

## TEST 4 — Roofing Agent: Price Question Early

**Agent speaks first:**
> Greeting message

**You:** Yeah go ahead

> Expected: HOOK delivery

**You:** How much does this cost?

> Expected: "Founding rate is $600/month, one contractor per ZIP. Most guys make that back on one job. But I'm getting ahead of myself — want to see the free leads first?"

**You:** Yeah okay show me

> Expected: Continues to SAMPLE OFFER, then follows normal flow.

### PASS if: Agent handled early price question, redirected to free sample leads, then continued normal script flow.

---

## TEST 5 — Remediation Agent: Happy Path

**Agent speaks first:**
> "Hi, is this the business owner? Great — my name is Alex, calling from Forced Action... water damage or fire remediation in Tampa..."

**You:** Yeah this is Sarah, what do you got?

> Expected: HOOK — "We monitor Hillsborough County for flood reports, fire incidents, and insurance adjuster inspections..."

**You:** We mostly do water damage, how does this work?

> Expected: BRIDGE — "We had 8 water and fire events this week... Three had insurance adjuster permits filed but no mitigation company contacted yet..."

**You:** That's actually really useful. Can I see some examples?

> Expected: SAMPLE OFFER — 3 free leads by text, no obligation.

**You:** Yes please

> Expected: "Perfect, sending those now — you'll get a text in about 60 seconds." Then FOUNDING CLOSE.

**You:** What's the catch?

> Expected: Founding rate $600/month explanation, ZIP exclusivity, locked for life.

**You:** Let me think about it. Can I talk to someone?

> Expected: DEMO CLOSE — quick call with Josh, 15 minutes, no commitment, sends scheduling link.

**You:** Sure

> Expected: "Sending it now." Warm goodbye.

### PASS if: Agent used remediation-specific language (flood, fire, adjuster permits, mitigation), not roofing language.

---

## TEST 6 — Remediation Agent: "We're Slammed" Objection

**Agent speaks first:**
> Greeting message

**You:** Yeah but we're super busy right now, not a good time

> Expected: "Perfect time to lock your ZIP then — if you're busy now, competitors will be too. The territory locks per company. Want me to text you the sample leads while you're working?"

**You:** Fine, send them

> Expected: Confirms sending, moves to founding close or goodbye.

### PASS if: Agent used the "slammed" objection handler, didn't just give up.

---

## TEST 7 — Remediation Agent: Insurance Adjuster Objection

**Agent speaks first:**
> Greeting message

**You:** Go ahead

> Expected: HOOK

**You:** We already get leads from insurance adjusters

> Expected: "Totally — this is actually upstream from that. We alert you when the adjuster permit gets filed, before the homeowner has made a single call. Want to see what that looks like for free?"

### PASS if: Agent used the "upstream" positioning correctly.

---

## RED FLAGS — Fail the test if any of these happen:

- [ ] Agent mentions CloudInnovate, project management, or cloud solutions
- [ ] Agent asks for your name instead of delivering the greeting
- [ ] Agent goes off-script with made-up features or pricing
- [ ] Agent promises specific revenue or ROI numbers
- [ ] Agent reads technical terms, field IDs, or variable names
- [ ] Agent keeps pushing after prospect says "not interested" twice
- [ ] Agent doesn't deliver the hook before jumping to the offer
- [ ] Roofing agent uses remediation language or vice versa
- [ ] Agent response exceeds 3 sentences (outside objection handling)
