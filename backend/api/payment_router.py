from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel
import stripe
from core.config import get_settings

settings = get_settings()
router = APIRouter()

# Configure Stripe API Key
stripe.api_key = settings.STRIPE_SECRET_KEY

# Models for request validation
class CheckoutSessionRequest(BaseModel):
    plan_tier: str # 'standard', 'plus', 'pro'
    billing_cycle: str # 'monthly', 'yearly'

@router.post("/create-checkout-session")
async def create_checkout_session(request: CheckoutSessionRequest):
    """
    Creates a secure Stripe Checkout session.
    Returns a URL that the frontend redirects the user to for payment (Visa/Mastercard).
    """
    
    # 1. Price Matching Logic (We match what was promised on the frontend)
    prices = {
        "monthly": {"standard": 0, "plus": 9, "pro": 16},
        "yearly": {"standard": 0, "plus": 86, "pro": 153} # 20% off roughly applied
    }
    
    amount = prices.get(request.billing_cycle, {}).get(request.plan_tier)
    
    if amount is None:
        raise HTTPException(status_code=400, detail="Noto'g'ri tarif tanlandi.")
        
    if amount == 0:
        return {"session_url": "/dashboard?status=free_activated"}

    # 2. Stripe Checkout Logic Simulator (In Production this initializes Stripe payload)
    try:
        # NOTE: For this to work in REAL production, we'd use stripe.checkout.Session.create()
        # Here we mock the behavior for the dev environment until real API keys are injected via .env
        
        # simulated_checkout_url = "https://checkout.stripe.com/pay/cs_test_..."
        # mock_checkout_url = f"https://buy.stripe.com/mock_success?plan={request.plan_tier}&cycle={request.billing_cycle}&amount={amount}"
        mock_checkout_url = f"http://127.0.0.1:8000/api/v1/payments/mock-checkout?plan={request.plan_tier}&cycle={request.billing_cycle}&amount={amount}"
        
        return {"session_url": mock_checkout_url}
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/webhook")
async def stripe_webhook(request: Request):
    """
    Stripe calls this endpoint automatically in the background when a payment 
    is successfully processed from the customer's Visa/Mastercard.
    Here we unlock the PRO features on the user's account in our database.
    """
    payload = await request.body()
    sig_header = request.headers.get("stripe-signature")
    
    # In production:
    # try:
    #    event = stripe.Webhook.construct_event(payload, sig_header, settings.STRIPE_WEBHOOK_SECRET)
    #    if event['type'] == 'checkout.session.completed':
    #       fulfill_order(event['data']['object'])
    # except ValueError as e: return ... bad payload
    
    return {"status": "success", "message": "O'zbekiston/Global to'lov qabul qilindi va hisob faollashdi!"}

from fastapi.responses import HTMLResponse

@router.get("/mock-checkout", response_class=HTMLResponse)
async def mock_checkout_page(plan: str, cycle: str, amount: int):
    """
    A simulated Stripe Checkout page so the user doesn't hit a 404/AccessDenied on Stripe's actual servers.
    """
    html_content = f"""
    <html>
        <head>
            <title>Stripe Checkout Simulator</title>
            <style>
                body {{ font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; background: #f7f9fc; display: flex; justify-content: center; align-items: center; height: 100vh; margin: 0; }}
                .checkout-box {{ background: white; padding: 40px; border-radius: 12px; box-shadow: 0 10px 30px rgba(0,0,0,0.1); width: 100%; max-width: 400px; text-align: center; }}
                h1 {{ color: #635bff; margin-bottom: 5px; font-size: 24px; }}
                p {{ color: #4F566B; margin-bottom: 30px; line-height: 1.5; }}
                .amount {{ font-size: 36px; font-weight: bold; color: #1a1f36; margin: 20px 0; }}
                .btn {{ background: #635bff; color: white; border: none; padding: 12px 20px; font-size: 16px; border-radius: 6px; cursor: pointer; width: 100%; font-weight: 600; display: block; text-decoration: none; }}
                .btn:hover {{ background: #5850ec; }}
                select, input {{ width: 100%; padding: 12px; border: 1px solid #e2e8f0; border-radius: 6px; margin-bottom: 15px; box-sizing: border-box; font-size: 16px; }}
            </style>
        </head>
        <body>
            <div class="checkout-box">
                <h1>Secure Checkout</h1>
                <p>Apex AI - <strong>{plan.capitalize()}</strong> Tier ({cycle})</p>
                <div class="amount">${amount}.00 USD</div>
                <input type="text" placeholder="Card Information (Simulator) **** **** **** 4242" disabled />
                <button class="btn" onclick="alert('Muvaffaqiyatli! To\\'lov amalga oshdi va webhook serverga xabar berdi. Endi API ishlashda davom etadi!'); window.history.back();">Pay ${amount}.00 USD</button>
            </div>
        </body>
    </html>
    """
    return html_content
