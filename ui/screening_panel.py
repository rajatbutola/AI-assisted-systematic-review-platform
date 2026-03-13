import streamlit as st

from storage.repository import ScreeningRepository, ArticleRepository

screen_repo = ScreeningRepository()
article_repo = ArticleRepository()


def render_screening_panel(review_id: int):

    st.subheader("Title/Abstract Screening")

    articles = article_repo.get_articles_for_review(review_id)

    if not articles:
        st.info("No articles to screen")
        return

    for article in articles:

        with st.container():

            st.markdown(f"### {article['title']}")
            st.write(article["journal"], article["year"])
            st.write(article["abstract"])

            col1, col2, col3 = st.columns(3)

            if col1.button("Include", key=f"inc_{article['pmid']}"):
                screen_repo.save_decision(
                    review_id,
                    article["pmid"],
                    "title_abstract",
                    "include",
                )
                st.rerun()

            if col2.button("Exclude", key=f"exc_{article['pmid']}"):
                screen_repo.save_decision(
                    review_id,
                    article["pmid"],
                    "title_abstract",
                    "exclude",
                )
                st.rerun()

            if col3.button("Unsure", key=f"uns_{article['pmid']}"):
                screen_repo.save_decision(
                    review_id,
                    article["pmid"],
                    "title_abstract",
                    "unsure",
                )
                st.rerun()

            st.divider()