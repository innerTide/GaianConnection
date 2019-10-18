import javax.swing.*;

public class DemoUI {
    private JTextField textFieldQuery;
    private JTextField textFieldUsername;
    private JButton buttonQuery;
    private JPanel rootPanel;
    private JLabel labelQueryResult;
    private JTable tableResult;

    public JTable getTableResult() {
        return tableResult;
    }

    public JTextField getTextFieldQuery() {
        return textFieldQuery;
    }

    public JTextField getTextFieldUsername() {
        return textFieldUsername;
    }

    public JButton getButtonQuery() {
        return buttonQuery;
    }

    public JPanel getRootPanel() {
        return rootPanel;
    }

    public JLabel getLabelQueryResult() {
        return labelQueryResult;
    }

}
