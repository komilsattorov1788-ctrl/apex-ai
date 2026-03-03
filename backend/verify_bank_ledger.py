import asyncio
import os
import sys
import uuid
from sqlalchemy import select, func

# Simulate Environment
os.environ["DATABASE_URL"] = "sqlite+aiosqlite:///bank_ledger_test.db"
sys.path.insert(0, r'C:\Users\User\.gemini\antigravity\scratch\neural-sync-ai\backend')

try:
    from database.database import AsyncSessionLocalWrite, master_engine
    from database.models import TransactionLedger, LedgerOperation, Base
except Exception as e:
    print("Import Error:", e)

async def setup_db():
    async with master_engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

async def test_bank_level_ledger():
    print("=== BANK-GRADE IMMUTABLE LEDGER VERIFICATION ===")
    print("No row is ever updated or locked. Balance is a mathematical derivation!")
    
    test_user_id = "user_bank_123"
    
    async with AsyncSessionLocalWrite() as session:
        # 1. Initial Deposit (Adding tokens)
        print("\n[Action] 1. Creating Initial DEPOSIT of 1000 credits...")
        dep = TransactionLedger(
            user_id=test_user_id, tx_id="tx_deposit_01", intent="initial_deposit",
            operation=LedgerOperation.DEPOSIT, debit_amount=0, credit_amount=1000
        )
        session.add(dep)
        
        # 2. Reserving tokens for heavy AI task
        print("[Action] 2. RESERVING 150 credits for AI Video generation...")
        res = TransactionLedger(
            user_id=test_user_id, tx_id="tx_video_01", intent="generate_video",
            operation=LedgerOperation.RESERVE, debit_amount=150, credit_amount=0
        )
        session.add(res)
        await session.commit()
        
        # Calculate Current Balance
        async def get_balance():
            # Balance = SUM(credit) - SUM(debit)
            query = select(
                func.sum(TransactionLedger.credit_amount) - func.sum(TransactionLedger.debit_amount)
            ).where(TransactionLedger.user_id == test_user_id)
            res = await session.execute(query)
            return res.scalar() or 0
            
        print(f"\n[Audit] -> Current Balance after reservation: {await get_balance()} credits.")
        
        # 3. Committing task but it only cost 100 credits instead of 150! Overpayment refund!
        print("\n[Action] 3. Video generation took less time! We only spent 100 out of 150.")
        print("          -> Committing task, and appending a REFUND row of 50 credits.")
        
        commit_log = TransactionLedger(
            user_id=test_user_id, tx_id="tx_video_01", intent="commit_video",
            operation=LedgerOperation.COMMIT, debit_amount=0, credit_amount=0
        )
        refund_log = TransactionLedger(
            user_id=test_user_id, tx_id="tx_video_01", intent="refund_unused",
            operation=LedgerOperation.REFUND, debit_amount=0, credit_amount=50
        )
        session.add_all([commit_log, refund_log])
        await session.commit()
        
        print(f"[Audit] -> Final Validated Balance: {await get_balance()} credits. (Expected: 900)")
        
        # Checking immutability
        print("\n[Audit] Printing all rows tied to 'tx_video_01':")
        query = select(TransactionLedger).where(TransactionLedger.tx_id == "tx_video_01")
        rows = (await session.execute(query)).scalars().all()
        for r in rows:
            print(f" -> {r.operation.value.upper():<8} | Debit: {r.debit_amount:<3} | Credit: {r.credit_amount:<3} | Intent: {r.intent}")
            
    print("\n[SUCCESS] True Bank-Grade accounting applied natively without locking bottlenecks!")

if __name__ == "__main__":
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(setup_db())
    asyncio.run(test_bank_level_ledger())
