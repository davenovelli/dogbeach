# Build from the repository root: docker build -f ml/tfidf/Dockerfile -t yewreview-tagger .
FROM python:3.12-slim-trixie
ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1
WORKDIR /app
COPY ml/tfidf/requirements.lock.txt /tmp/requirements.txt
RUN python -m pip install --no-cache-dir --upgrade pip==26.2.2 && pip install --no-cache-dir -r /tmp/requirements.txt && useradd -m -u 1000 tagger
COPY --chown=1000:1000 dogbeach/__init__.py dogbeach/api.py /app/dogbeach/
COPY --chown=1000:1000 ml/tfidf/autotagging.py ml/tfidf/apostrophe_terms_to_save.txt ml/tfidf/stopwords.txt /app/ml/tfidf/
WORKDIR /app/ml/tfidf
USER tagger
ENTRYPOINT ["python", "autotagging.py"]
CMD ["--help"]
