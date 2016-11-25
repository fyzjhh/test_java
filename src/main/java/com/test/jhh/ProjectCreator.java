package com.test.jhh;

import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.WindowEvent;
import java.awt.event.WindowListener;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Date;
import java.util.Properties;

import javax.swing.JButton;
import javax.swing.JFileChooser;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JOptionPane;
import javax.swing.JTextField;
import javax.swing.filechooser.FileFilter;

public class ProjectCreator {

	public static void main(String[] args) throws IOException {

		final JFrame frame = new JFrame("ProjectCreator");
		frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
		frame.setSize(600, 400);
		frame.setLocation(200, 100);

		frame.getContentPane().setLayout(new java.awt.GridBagLayout());

		JLabel lb = new JLabel();
		lb.setText("Select project path:");

		final JTextField path = new JTextField(20);

		JButton btnBrows = new JButton("...");
		btnBrows.addActionListener(new ActionListener() {

			public void actionPerformed(ActionEvent e) {
				JFileChooser dlg = new JFileChooser(path.getText());
				FileFilter filter = new FileFilter() {

					public String getDescription() {
						return null;
					}

					public boolean accept(File f) {
						return f.isDirectory();
					}
				};
				dlg.setFileFilter(filter);
				dlg.setFileSelectionMode(JFileChooser.DIRECTORIES_ONLY);
				if (dlg.showOpenDialog(frame) == JFileChooser.APPROVE_OPTION) {
					try {
						path.setText(dlg.getSelectedFile().getCanonicalPath());
					} catch (IOException e1) {
						e1.printStackTrace();
					}
				}
			}
		});

		JButton btnRun = new JButton();
		btnRun.setLocation(60, 60);
		btnRun.setText("Run");
		btnRun.addActionListener(new ActionListener() {

			public void actionPerformed(ActionEvent arg0) {
				try {

					// key function
					new ProjectProc().process(path.getText());

					JOptionPane.showMessageDialog(frame, "finished");

				} catch (Exception e) {
					JOptionPane.showMessageDialog(frame, e.toString());
				}
			}
		});

		frame.getContentPane().add(lb);
		frame.getContentPane().add(path);
		frame.getContentPane().add(btnBrows);
		frame.getContentPane().add(btnRun);
		frame.setVisible(true);

		final Properties prop = new Properties();
		try {
			prop.load(new FileInputStream(ProjectProc.getConfigFileName()));
		} catch (IOException e) {
			e.printStackTrace();
		}
		path.setText(prop.getProperty("path"));

		frame.addWindowListener(new WindowListener() {

			public void windowClosing(WindowEvent arg0) {

				try {
					prop.setProperty("path", path.getText());
					String file = ProjectProc.getConfigFileName();
					prop.store(new FileOutputStream(file),
							(new Date()).toString());
				} catch (IOException e) {

					e.printStackTrace();
				}
			}

			public void windowActivated(WindowEvent e) {

			}

			public void windowClosed(WindowEvent e) {

			}

			public void windowDeactivated(WindowEvent e) {

			}

			public void windowDeiconified(WindowEvent e) {

			}

			public void windowIconified(WindowEvent e) {

			}

			public void windowOpened(WindowEvent e) {

			}

		});
	}

}