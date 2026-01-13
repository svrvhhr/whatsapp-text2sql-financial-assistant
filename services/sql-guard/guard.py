def validate_sql(sql: str, role: str) -> bool:
    """
    Retourne True si SQL autorisé selon le rôle
    """
    sql_lower = sql.lower()
    if role == "lecture_seule":
        return sql_lower.startswith("select")
    if role == "responsable_projet":
        # limiter tables sensibles
        forbidden = ["entreprises", "transferts"]
        return not any(t in sql_lower for t in forbidden)
    if role == "admin_financier":
        return True
    return False
