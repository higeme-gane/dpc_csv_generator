# DPC6桁コード別 病院別患者数集計ツール

GitHub Pages + GitHub Actions で誰でも簡単にCSV生成できます。

## 使い方
1. [GitHub Pages](https://あなたのユーザー名.github.io/dpc-csv-generator/) にアクセス
2. フォームで `dpc6` と `all_0_ope_1` を入力
3. 表示されるリンクから **Actions** を実行
4. 完了後、artifactからCSVをダウンロード

## データ配置（最初に一度だけ）
- 10個の parquet ファイルをリポジトリ直下に置いてください
- 大容量ファイルなので **Git LFS** を使用してください：
  ```bash
  git lfs install
  git lfs track "*.parquet"
  git add .gitattributes
  git commit -m "Add parquet files with LFS"