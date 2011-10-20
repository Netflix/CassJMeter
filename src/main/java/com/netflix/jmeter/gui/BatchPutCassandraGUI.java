package com.netflix.jmeter.gui;

import java.awt.BorderLayout;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;

import javax.swing.JComboBox;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JTextArea;
import javax.swing.JTextField;
import javax.swing.border.BevelBorder;

import org.apache.jmeter.testelement.TestElement;

import com.netflix.jmeter.sampler.AbstractCassandraSampler;
import com.netflix.jmeter.sampler.BatchPutSampler;

public class BatchPutCassandraGUI extends AbstractCassandraGUI
{
    private static final long serialVersionUID = 3197090412869386190L;
    public static String LABEL = "Cassandra Batch Put";
    private JTextField KEY;
    private JTextArea NAME_AND_VALUE;
    private JComboBox KSERIALIZER;
    private JComboBox CSERIALIZER;
    private JComboBox VSERIALIZER;

    @Override
    public String getStaticLabel()
    {
        return LABEL;
    }
    
    public String getLabelResource()
    {
        return LABEL;
    }

    @Override
    public void configure(TestElement element)
    {
        super.configure(element);
        KEY.setText(element.getPropertyAsString(BatchPutSampler.KEY));
        NAME_AND_VALUE.setText(element.getPropertyAsString(BatchPutSampler.NAME_AND_VALUE));
        KSERIALIZER.setSelectedItem(element.getPropertyAsString(BatchPutSampler.KEY_SERIALIZER_TYPE));
        CSERIALIZER.setSelectedItem(element.getPropertyAsString(BatchPutSampler.COLUMN_SERIALIZER_TYPE));
        VSERIALIZER.setSelectedItem(element.getPropertyAsString(BatchPutSampler.VALUE_SERIALIZER_TYPE));
    }

    public TestElement createTestElement()
    {
        BatchPutSampler sampler = new BatchPutSampler();
        modifyTestElement(sampler);
        sampler.setComment("test comment");
        return sampler;
    }

    public void modifyTestElement(TestElement sampler)
    {
        super.configureTestElement(sampler);

        if (sampler instanceof BatchPutSampler)
        {
            BatchPutSampler gSampler = (BatchPutSampler) sampler;
            gSampler.setKSerializerType((String) KSERIALIZER.getSelectedItem());
            gSampler.setCSerializerType((String) CSERIALIZER.getSelectedItem());
            gSampler.setVSerializerType((String) VSERIALIZER.getSelectedItem());
            gSampler.setNameValue(NAME_AND_VALUE.getText());
            gSampler.setKey(KEY.getText());
        }
    }

    public void initFields()
    {

        KEY.setText("${__Random(1,1000)}");
        NAME_AND_VALUE.setText("${__Random(1,1000)}:${__Random(1,1000)}\n${__Random(1,1000)}:${__Random(1,1000)}");
        KSERIALIZER.setSelectedItem("Key Serializer");
        CSERIALIZER.setSelectedItem("Column Serializer");
        VSERIALIZER.setSelectedItem("Value Serializer");
    }

    public void init()
    {
        setLayout(new BorderLayout(0, 5));
        setBorder(makeBorder());
        JPanel mainPanel = new JPanel(new GridBagLayout());
        GridBagConstraints labelConstraints = new GridBagConstraints();
        labelConstraints.anchor = GridBagConstraints.FIRST_LINE_END;

        GridBagConstraints editConstraints = new GridBagConstraints();
        editConstraints.anchor = GridBagConstraints.FIRST_LINE_START;
        editConstraints.weightx = 1.0;
        editConstraints.fill = GridBagConstraints.HORIZONTAL;

        addToPanel(mainPanel, labelConstraints, 0, 0, new JLabel("ROW KEY: ", JLabel.RIGHT));
        addToPanel(mainPanel, editConstraints, 1, 0, KEY = new JTextField());
        addToPanel(mainPanel, labelConstraints, 0, 1, new JLabel("COLUMN NAME AND VALUE (eg: NAME:VALUE): ", JLabel.RIGHT));
        addToPanel(mainPanel, editConstraints, 1, 1, NAME_AND_VALUE = new JTextArea());
        NAME_AND_VALUE.setRows(10);
        NAME_AND_VALUE.setBorder(new BevelBorder(BevelBorder.LOWERED));
        addToPanel(mainPanel, labelConstraints, 0, 2, new JLabel("Key Serializer: ", JLabel.RIGHT));
        addToPanel(mainPanel, editConstraints, 1, 2, KSERIALIZER = new JComboBox(AbstractCassandraSampler.getSerializerNames().toArray()));
        addToPanel(mainPanel, labelConstraints, 0, 3, new JLabel("Column Serializer: ", JLabel.RIGHT));
        addToPanel(mainPanel, editConstraints, 1, 3, CSERIALIZER = new JComboBox(AbstractCassandraSampler.getSerializerNames().toArray()));
        addToPanel(mainPanel, labelConstraints, 0, 4, new JLabel("Value Serializer: ", JLabel.RIGHT));
        addToPanel(mainPanel, editConstraints, 1, 4, VSERIALIZER = new JComboBox(AbstractCassandraSampler.getSerializerNames().toArray()));

        JPanel container = new JPanel(new BorderLayout());
        container.add(mainPanel, BorderLayout.NORTH);
        add(container, BorderLayout.CENTER);
    }
}
