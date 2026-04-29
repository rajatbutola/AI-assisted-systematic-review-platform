"""
Run: python debug_s1.py 1
"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from storage.database import get_connection

def debug(review_id: int):
    with get_connection() as conn:

        # Exact Stage 1 adjudications
        adj = conn.execute("""
            SELECT pmid, final_decision FROM adjudications
            WHERE review_id=? AND stage='title_abstract'
        """, (review_id,)).fetchall()
        adj_map = {r[0]: r[1] for r in adj}
        print(f"S1 adjudications total: {len(adj)}")
        from collections import Counter
        adj_dec_counts = Counter(r[1] for r in adj)
        print(f"  by decision: {dict(adj_dec_counts)}")

        # Exact Stage 1 reviewer decisions grouped by pmid
        rev = conn.execute("""
            SELECT pmid,
                   GROUP_CONCAT(DISTINCT decision) AS decisions,
                   COUNT(DISTINCT reviewer_id)     AS n_reviewers
            FROM screening_decisions
            WHERE review_id=? AND stage='title_abstract'
            AND reviewer_id NOT IN ('final_resolved','editor','adjudicator')
            GROUP BY pmid
        """, (review_id,)).fetchall()

        # Simulate new get_screening_counts logic
        decision_counts = {"include":0,"exclude":0,"unsure":0,"conflict":0}
        counted = set()

        # Priority 1: adjudicated
        for pmid, dec in adj_map.items():
            decision_counts[dec] = decision_counts.get(dec,0) + 1
            counted.add(pmid)

        # Priority 2: consensus
        for row in rev:
            pmid = row[0]
            if pmid in counted: continue
            decs = set(d.strip() for d in (row[1] or "").split(",") if d.strip())
            n_rev = row[2]
            if len(decs)==1 and n_rev>=2:
                dec = decs.pop()
                decision_counts[dec] = decision_counts.get(dec,0)+1
                counted.add(pmid)
            elif len(decs)>1:
                decision_counts["conflict"] += 1
                counted.add(pmid)

        print(f"\nSimulated get_screening_counts:")
        print(f"  include={decision_counts['include']}")
        print(f"  exclude={decision_counts['exclude']}")
        print(f"  unsure={decision_counts['unsure']}")
        print(f"  conflict={decision_counts['conflict']}")
        print(f"  counted={len(counted)}")

        # Simulate get_stage1_included_pmids
        adj_inc = {r[0] for r in adj if r[1]=='include'}
        cons_inc = set()
        for row in rev:
            pmid=row[0]
            if pmid in adj_map: continue
            decs=set(d.strip() for d in (row[1] or "").split(",") if d.strip())
            if len(decs)==1 and decs=={"include"} and row[2]>=2:
                cons_inc.add(pmid)
        eligible = adj_inc | cons_inc
        print(f"\nget_stage1_included_pmids: {len(eligible)}")
        print(f"  adj include: {len(adj_inc)}")
        print(f"  consensus include: {len(cons_inc)}")

        # Find the discrepancy
        diff = eligible - counted
        print(f"\nIn eligible but NOT in counted: {diff}")
        diff2 = {p for p in counted if p not in eligible and decision_counts.get('include',0)}
        
        # Show any pmid in adj_inc that also has reviewer consensus include
        overlap = adj_inc & cons_inc
        print(f"PMIDs in BOTH adj_inc and cons_inc (overlap): {overlap}")

        # Show all adj with final_decision=include
        print(f"\nAll S1 adj with final_decision=include ({len(adj_inc)}):")
        for pmid in sorted(adj_inc):
            rev_row = next((r for r in rev if r[0]==pmid), None)
            rev_decs = rev_row[1] if rev_row else "no_reviewer_rows"
            print(f"  {pmid}: adj=include  reviewer_decs={rev_decs}")

if __name__=="__main__":
    rid = int(sys.argv[1]) if len(sys.argv)>1 else 1
    debug(rid)