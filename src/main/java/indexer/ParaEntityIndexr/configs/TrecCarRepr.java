package main.java.indexer.ParaEntityIndexr.configs;

public interface TrecCarRepr {

    enum TrecCarSearchField {
        Id(0), Text(1),Headings(2), Title(3), AnchorNames(4), DisambiguationNames(5), CategoryNames(6)
        , InlinkIds(7), OutlinkIds(8), EntityLinks(9), Entity(10), LeadText(11), BiText(12), WText(13) ;

        private int value;
        private TrecCarSearchField(int value) {
            this.value = value;
        }

        private static TrecCarSearchField[] values = null;
        public static TrecCarSearchField fromInt(int i) {
            if (TrecCarSearchField.values == null) {
                TrecCarSearchField.values = TrecCarSearchField.values();
            }
            return TrecCarSearchField.values[i];
        }
    }
    TrecCarSearchField getIdField();
    TrecCarSearchField getTextField();
    TrecCarSearchField getEntityField();
    TrecCarSearchField[] getSearchFields();
}
