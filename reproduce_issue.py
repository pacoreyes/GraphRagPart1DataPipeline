import msgspec
from data_pipeline.models import Article, ArticleMetadata
from data_pipeline.utils.chroma_helpers import generate_doc_id

def test_logic():
    # Simulate an article
    metadata = ArticleMetadata(
        title="Test",
        artist_name="Artist",
        country="US",
        wikipedia_url="url",
        wikidata_uri="uri",
        chunk_index=1,
        total_chunks=1
    )
    article = Article(id="Q123", metadata=metadata, article="Some content")
    
    # Intentionally broken to see error message
    doc_id = generate_doc_id(article, article.id)
    print(f"Generated ID: {doc_id}")

if __name__ == "__main__":
    test_logic()