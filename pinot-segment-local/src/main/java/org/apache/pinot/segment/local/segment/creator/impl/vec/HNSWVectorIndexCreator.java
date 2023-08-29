package org.apache.pinot.segment.local.segment.creator.impl.vec;

import java.io.File;
import java.io.IOException;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.pinot.segment.local.realtime.impl.invertedindex.RealtimeLuceneTextIndex;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentColumnarIndexCreator;
import org.apache.pinot.segment.spi.V1Constants;
import org.apache.pinot.segment.spi.index.creator.VectorIndexCreator;
import org.apache.pinot.spi.data.readers.Vector;


/**
 * This is used to create Lucene based text index.
 * Used for both offline from {@link SegmentColumnarIndexCreator}
 * and realtime from {@link RealtimeLuceneTextIndex}
 */
public class HNSWVectorIndexCreator implements VectorIndexCreator {
  public static final String VECTOR_INDEX_DOC_ID_COLUMN_NAME = "DocID";

  private final Directory _indexDirectory;
  private final IndexWriter _indexWriter;
  private final String _vectorColumn;

  private int _nextDocId = 0;

  public HNSWVectorIndexCreator(String column, File segmentIndexDir, boolean commit, boolean useCompoundFile,
      int maxBufferSizeMB,  int vectorLength, int vectorValueSize) {
    _vectorColumn = column;
    try {
      // segment generation is always in V1 and later we convert (as part of post creation processing)
      // to V3 if segmentVersion is set to V3 in SegmentGeneratorConfig.
      File indexFile = getV1VectorIndexFile(segmentIndexDir);
      _indexDirectory = FSDirectory.open(indexFile.toPath());
      System.out.println("Creating HNSW index for column: " + column +" at path: " + indexFile.getAbsolutePath());


      IndexWriterConfig indexWriterConfig = new IndexWriterConfig();
      indexWriterConfig.setRAMBufferSizeMB(maxBufferSizeMB);
      indexWriterConfig.setCommitOnClose(commit);
      indexWriterConfig.setUseCompoundFile(useCompoundFile);
      _indexWriter = new IndexWriter(_indexDirectory, indexWriterConfig);
    } catch (Exception e) {
      throw new RuntimeException(
          "Caught exception while instantiating the LuceneTextIndexCreator for column: " + column, e);
    }
  }

  public IndexWriter getIndexWriter() {
    return _indexWriter;
  }

  @Override
  public void add(@Nonnull Object value, int dictId)
      throws IOException {
    add((Vector) value);
  }

  @Override
  public void add(@Nonnull Object[] values, @Nullable int[] dictIds) {
    if (values instanceof Vector[]) {
      add((Vector[]) values, values.length);
    } else {
      Vector[] vectors = new Vector[values.length];
      for (int i = 0; i < values.length; i++) {
        vectors[i] = (Vector) values[i];
      }
      add(vectors, values.length);
    }
  }

  @Override
  public void add(Vector document) {
    // text index on SV column
    Document docToIndex = new Document();
    docToIndex.add(new XKnnFloatVectorField(_vectorColumn, document.getFloatValues(), VectorSimilarityFunction.COSINE));
    docToIndex.add(new StoredField(VECTOR_INDEX_DOC_ID_COLUMN_NAME, _nextDocId++));

    try {
      _indexWriter.addDocument(docToIndex);
    } catch (Exception e) {
      throw new RuntimeException(
          "Caught exception while adding a new document to the Lucene index for column: " + _vectorColumn, e);
    }
  }

  public void add(Vector[] documents, int length) {
    Document docToIndex = new Document();

    // Whenever multiple fields with the same name appear in one document, both the
    // inverted index and term vectors will logically append the tokens of the
    // field to one another, in the order the fields were added.
    for (int i = 0; i < length; i++) {
      docToIndex.add(new XKnnFloatVectorField(_vectorColumn, documents[i].getFloatValues(), VectorSimilarityFunction.COSINE));
    }
    docToIndex.add(new StoredField(VECTOR_INDEX_DOC_ID_COLUMN_NAME, _nextDocId++));

    try {
      _indexWriter.addDocument(docToIndex);
    } catch (Exception e) {
      throw new RuntimeException(
          "Caught exception while adding a new document to the Lucene index for column: " + _vectorColumn, e);
    }
  }

  @Override
  public void seal() {
    try {
      System.out.println("Sealing HNSW index for column: " + _vectorColumn);
      _indexWriter.forceMerge(1);
    } catch (Exception e) {
      throw new RuntimeException("Caught exception while sealing the Lucene index for column: " + _vectorColumn, e);
    }
  }

  @Override
  public void close()
      throws IOException {
    try {
      // based on the commit flag set in IndexWriterConfig, this will decide to commit or not
      _indexWriter.close();
      _indexDirectory.close();
    } catch (Exception e) {
      throw new RuntimeException("Caught exception while closing the HNSW index for column: " + _vectorColumn, e);
    }
  }

  private File getV1VectorIndexFile(File indexDir) {
    String luceneIndexDirectory = _vectorColumn + V1Constants.Indexes.VECTOR_HNSW_INDEX_FILE_EXTENSION;
    return new File(indexDir, luceneIndexDirectory);
  }
}