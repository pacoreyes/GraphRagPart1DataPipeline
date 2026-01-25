from unittest.mock import MagicMock, patch

from langchain_text_splitters import RecursiveCharacterTextSplitter

from data_pipeline.utils.data_transformation_helpers import (
    format_list_natural_language,
    create_rag_text_splitter,
)

def test_format_list_natural_language_empty():
    assert format_list_natural_language([]) == ""
    assert format_list_natural_language(None) == ""

def test_format_list_natural_language_single():
    assert format_list_natural_language(["apple"]) == "apple"

def test_format_list_natural_language_two():
    assert format_list_natural_language(["apple", "banana"]) == "apple and banana"

def test_format_list_natural_language_multiple():
    assert format_list_natural_language(["apple", "banana", "cherry"]) == "apple, banana, and cherry"
    assert format_list_natural_language(["A", "B", "C", "D"]) == "A, B, C, and D"

def test_format_list_natural_language_deduplication():
    # Should maintain order of first appearance
    assert format_list_natural_language(["apple", "banana", "apple"]) == "apple and banana"

def test_format_list_natural_language_non_strings():
    assert format_list_natural_language([1, 2, 3]) == "1, 2, and 3"


class TestCreateRagTextSplitter:
    """Tests for the create_rag_text_splitter factory function."""

    @patch("data_pipeline.utils.data_transformation_helpers.AutoTokenizer")
    @patch("data_pipeline.utils.data_transformation_helpers.RecursiveCharacterTextSplitter")
    def test_create_rag_text_splitter_returns_splitter(
        self, mock_splitter_class, mock_tokenizer_class
    ):
        """Test that factory returns a RecursiveCharacterTextSplitter instance."""
        mock_tokenizer = MagicMock()
        mock_tokenizer_class.from_pretrained.return_value = mock_tokenizer

        mock_splitter = MagicMock(spec=RecursiveCharacterTextSplitter)
        mock_splitter_class.from_huggingface_tokenizer.return_value = mock_splitter

        result = create_rag_text_splitter(
            model_name="test-model",
            chunk_size=512,
            chunk_overlap=50,
        )

        assert result == mock_splitter
        mock_tokenizer_class.from_pretrained.assert_called_once_with(
            "test-model", trust_remote_code=True
        )
        mock_splitter_class.from_huggingface_tokenizer.assert_called_once_with(
            mock_tokenizer,
            chunk_size=512,
            chunk_overlap=50,
            separators=["\n\n", "\n", ". ", "? ", "! ", " ", ""],
        )

    @patch("data_pipeline.utils.data_transformation_helpers.AutoTokenizer")
    @patch("data_pipeline.utils.data_transformation_helpers.RecursiveCharacterTextSplitter")
    def test_create_rag_text_splitter_passes_parameters(
        self, mock_splitter_class, mock_tokenizer_class
    ):
        """Test that chunk_size and chunk_overlap are passed correctly."""
        mock_tokenizer = MagicMock()
        mock_tokenizer_class.from_pretrained.return_value = mock_tokenizer
        mock_splitter_class.from_huggingface_tokenizer.return_value = MagicMock()

        create_rag_text_splitter(
            model_name="nomic-ai/nomic-embed-text-v1.5",
            chunk_size=1024,
            chunk_overlap=100,
        )

        call_kwargs = mock_splitter_class.from_huggingface_tokenizer.call_args
        assert call_kwargs.kwargs["chunk_size"] == 1024
        assert call_kwargs.kwargs["chunk_overlap"] == 100
