from table_models import engine

def import_comments():
    conn = engine().connect()
    result = conn.execute('''
            SELECT
        c.id AS comment_id,
        c.comment AS comment,
        u.id AS user_id,
        c.date AS date,
        COUNT(cl.comment_id) AS likes,
        JSON_AGG(
            JSON_BUILD_OBJECT(
            'id', cr.id,
            'comment', cr.comment,
            'user', JSON_BUILD_OBJECT('id', cu.id),
            'date', cr.date,
            'likes', COALESCE(clr.likes, 0)
            ) ORDER BY cr.date ASC
        ) AS replies
        FROM
        comments c
        LEFT JOIN comment_replies cr ON c.id = cr.parent_comment_id
        LEFT JOIN users u ON c.user_id = u.id
        LEFT JOIN (
            SELECT comment_id, user_id, COUNT(*) AS likes
            FROM comment_likes
            GROUP BY comment_id, user_id
        ) cl ON c.id = cl.comment_id
        LEFT JOIN users cu ON cl.user_id = cu.id
        LEFT JOIN (
            SELECT comment_id, COUNT(*) AS likes
            FROM comment_reply_likes
            GROUP BY  comment_id
        ) clr ON cr.id = clr.comment_id
        GROUP BY
        c.id, c.comment, u.id, c.date;
    ''')
    comments = []
    for row in result:
        comment = {
            'id': row[0],
            'comment': row[1],
            'user_id': row[2],
            'date': row[3].isoformat(),
            'likes': row[4],
            'replies': row[5]
        }
        comments.append(comment)
    if len(comments) == 0:
        comments = [{'id':2715,
            'comment':'placeholder',
            'user_id': 0,
            'date': None,
            'likes': None,
            'replies': [{'comment':'placeholder', 'id':0}],
        }]
    return comments
